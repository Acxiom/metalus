package com.acxiom.metalus.applications

import com.acxiom.metalus._
import com.acxiom.metalus.context.{ContextManager, Json4sContext, SessionContext, StepStatus}
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.utils.ReflectionUtils
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
 * Provides a set of utility functions for working with Application metadata
 */
object ApplicationUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * This function will convert the Application into an execution plan supplying the globals (if provided) to each
    * PipelineContext. This function will create a SparkSession using the provided SparkConf.
    *
    * @param application        The Application to use to generate the execution plan.
    * @param globals            An optional set of globals to use in each PipelineContext
    * @param parameters         The parameters used to initialize the application.
    * @param pipelineListener   An optional PipelineListener. This may be overridden by the application.
    * @param credentialProvider The credential provider.
    * @return A PipelineContext.
    */
  def createPipelineContext(application: Application,
                            globals: Option[Map[String, Any]],
                            parameters: Option[Map[String, Any]],
                            pipelineListener: PipelineListener = PipelineListener(),
                            credentialProvider: Option[CredentialProvider] = None): PipelineContext = {
    logger.info("Building Pipeline Context")
    if (application.pipelineId.isEmpty || application.pipelineId.getOrElse("").isEmpty) {
      throw new IllegalArgumentException("Application pipelineId is required!")
    }
    val executionEngines = parameters.getOrElse(Map()).getOrElse("executionEngines", "").toString.split(",").map(_.trim) :+ "batch"
    val validateArgumentTypes = parameters.getOrElse(Map()).getOrElse("validateStepParameterTypes", false).asInstanceOf[Boolean]
    // Create the ContextManager
    // The top level context can be a ClassInfo, but the parameters will be hydrated
    val preCtx: PipelineContext = PipelineContext(globals, List(), contextManager = new ContextManager(Map(), Map()))
    val contexts = application.contexts.getOrElse(Map()).map(context => {
      val classInfo = context._2
      context._1 -> classInfo.copy(parameters = Some(parseParameters(classInfo, credentialProvider, preCtx)))
    })
    val contextManager = new ContextManager(contexts,
      parameters.getOrElse(Map()) + ("credentialProvider" -> credentialProvider))
    val sessionContext = contextManager.getContext("session").get.asInstanceOf[SessionContext]
    val audits = sessionContext.loadAudits().getOrElse(List())
    val stepResults = sessionContext.loadStepResults().getOrElse(Map())
      .map(r => (PipelineStateKey.fromString(r._1), r._2))
    val stepStatus = sessionContext.loadStepStatus()
    val sessionGlobals = sessionContext.loadGlobals(PipelineStateKey(application.pipelineId.getOrElse(""))).getOrElse(Map())
    val tempCtx = PipelineContext(globals, List(), contextManager = contextManager)
    val globalStepMapper = generateStepMapper(application.stepMapper, Some(PipelineStepMapper()),
      validateArgumentTypes, credentialProvider, tempCtx)
    val rootGlobals = globals.getOrElse(Map[String, Any]()) // Create the default globals
    val globalListener = generatePipelineListener(application.pipelineListener, Some(pipelineListener),
      validateArgumentTypes, credentialProvider, tempCtx)
    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(List[PipelineParameter]()))
    val pipelineManager = generatePipelineManager(application.pipelineManager,
      Some(PipelineManager(application.pipelineTemplates)),
      validateArgumentTypes, credentialProvider, tempCtx).get
    val initialContext = PipelineContext(Some(rootGlobals), globalPipelineParameters.get, application.stepPackages,
      globalStepMapper.get, globalListener, audits, pipelineManager, credentialProvider, contextManager, stepResults, None,
      executionEngines = Some(executionEngines.toList), stepStatus = stepStatus)
    val restartPoints = getRestartPoints(parameters.getOrElse(Map()), sessionContext, initialContext)
    val defaultGlobals = generateGlobals(application.globals, rootGlobals , Some(rootGlobals), initialContext)
    initialContext.copy(globals = Some(sessionGlobals ++ defaultGlobals.get), restartPoints = restartPoints)
  }

  private def getRestartPoints(parameters: Map[String, Any], sessionContext: SessionContext, pipelineContext: PipelineContext): Option[RestartPoints] = {
    val history = sessionContext.sessionHistory
    val stepStatus = pipelineContext.stepStatus.getOrElse(List())
    val failedSteps = stepStatus.filter(_.status == "RUNNING")
    if (!parameters.getOrElse("disableRecovery", false).toString.toBoolean && history.isDefined && history.get.nonEmpty &&
      pipelineContext.stepStatus.isDefined && pipelineContext.stepStatus.get.nonEmpty && failedSteps.nonEmpty) {
      // Walk each step back to the restartable step
      val restarts = failedSteps.flatMap(step => getRestartableStep(pipelineContext, PipelineStateKey.fromString(step.stepKey)))
      if (restarts.nonEmpty) {
        Some(RestartPoints(restarts))
      } else {
        None
      }
    } else {
      // See if this run is needs to start at certain points
      val stepList = parameters.getOrElse("restartSteps", "").toString.split(",")
      if (stepList.nonEmpty && stepList.head.nonEmpty) {
        val keyMap = stepList.map(step => {
          val info = PipelineStateKey.fromString(step)
          val pipeline = pipelineContext.pipelineManager.getPipeline(info.pipelineId)
          if (pipeline.isEmpty) {
            throw new IllegalArgumentException(s"Unable to load pipeline ${info.pipelineId}!")
          }
          val allowedRestarts = pipeline.get.parameters.getOrElse(Parameters()).restartableSteps
          if (!allowedRestarts.exists(_.contains(info.stepId.getOrElse("NOPE")))) {
            throw new IllegalArgumentException(s"Step is not restartable: ${info.key}")
          }
          StepState(info, "RESTART")
        })
        Some(RestartPoints(keyMap.toList))
      } else {
        None
      }
    }
  }

  /**
   * This function will take the provided StepStatus and find the step that can be used as a restart point.
   *
   * @param pipelineContext The current PipelineContext
   * @param stepKey         The unique key fo the step.
   * @return A StepState object to be used for restarts.
   */
  @tailrec
  private def getRestartableStep(pipelineContext: PipelineContext, stepKey: PipelineStateKey): Option[StepState] = {
    val pipeline = pipelineContext.pipelineManager.getPipeline(stepKey.pipelineId)
    if (pipeline.isEmpty) {
      throw PipelineException(
        message = Some(s"Unable to load pipeline by id (${stepKey.pipelineId}) while determining restart points!"), pipelineProgress = None)
    }
    val stepStatus = pipelineContext.stepStatus.get
    val allowedSteps = pipeline.get.parameters.getOrElse(Parameters()).restartableSteps.getOrElse(List())
      .map(s => stepKey.copy(stepId = Some(s), forkData = None).key)
    // Walk the stepStatus
    if (allowedSteps.contains(stepKey.copy(forkData = None).key)) {
      Some(StepState(stepKey, "RESTART"))
    } else {
      val state = findRestartableStep(stepKey, allowedSteps, stepStatus)
      if (state.isDefined) {
        Some(StepState(state.get, "RESTART"))
      } else if (stepKey.stepGroup.isEmpty) {
        None
      } else {
        getRestartableStep(pipelineContext, stepKey.stepGroup.get)
      }
    }
  }

  @tailrec
  private def findRestartableStep(stepKey: PipelineStateKey, allowedSteps: List[String], stepStatus: List[StepStatus]): Option[PipelineStateKey] = {
    val forkLessStepKey = stepKey.copy(forkData = None)
    val previous = stepStatus.find(status => status.nextSteps.isDefined && status.nextSteps.get.contains(forkLessStepKey.stepId.getOrElse("")))
    if (previous.isDefined) {
      val key = PipelineStateKey.fromString(previous.get.stepKey).copy(forkData = stepKey.forkData)
      if (allowedSteps.contains(previous.get.stepKey)) {
        Some(key)
      } else {
        findRestartableStep(key, allowedSteps, stepStatus)
      }
    } else {
      None
    }
  }

  private def generatePipelineManager(pipelineManagerInfo: Option[ClassInfo],
                                      pipelineManager: Option[PipelineManager],
                                      validateArgumentTypes: Boolean,
                                      credentialProvider: Option[CredentialProvider],
                                      pipelineContext: PipelineContext): Option[PipelineManager] = {
    if (pipelineManagerInfo.isDefined && pipelineManagerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineManagerInfo.get.className.getOrElse("com.acxiom.metalus.CachedPipelineManager"),
        Some(parseParameters(pipelineManagerInfo.get, credentialProvider, pipelineContext)), validateArgumentTypes).asInstanceOf[PipelineManager])
    } else {
      pipelineManager
    }
  }

  private def generatePipelineListener(pipelineListenerInfo: Option[ClassInfo],
                                       pipelineListener: Option[PipelineListener],
                                       validateArgumentTypes: Boolean,
                                       credentialProvider: Option[CredentialProvider],
                                       pipelineContext: PipelineContext): Option[PipelineListener] = {
    if (pipelineListenerInfo.isDefined && pipelineListenerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineListenerInfo.get.className.getOrElse("com.acxiom.metalus.DefaultPipelineListener"),
        Some(parseParameters(pipelineListenerInfo.get, credentialProvider, pipelineContext)), validateArgumentTypes).asInstanceOf[PipelineListener])
    } else {
      pipelineListener
    }
  }

  private def generateStepMapper(stepMapperInfo: Option[ClassInfo],
                                 stepMapper: Option[PipelineStepMapper],
                                 validateArgumentTypes: Boolean,
                                 credentialProvider: Option[CredentialProvider],
                                 pipelineContext: PipelineContext): Option[PipelineStepMapper] = {
    if (stepMapperInfo.isDefined && stepMapperInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(stepMapperInfo.get.className.getOrElse("com.acxiom.metalus.DefaultPipelineStepMapper"),
        Some(parseParameters(stepMapperInfo.get, credentialProvider, pipelineContext)), validateArgumentTypes).asInstanceOf[PipelineStepMapper])
    } else {
      stepMapper
    }
  }

  private def generatePipelineParameters(pipelineParameters: Option[List[PipelineParameter]],
                                         defaultPipelineParameters: Option[List[PipelineParameter]]): Option[List[PipelineParameter]] = {
    if (pipelineParameters.isDefined && pipelineParameters.get.nonEmpty) {
      pipelineParameters
    } else {
      defaultPipelineParameters
    }
  }

  private def generateGlobals(globals: Option[Map[String, Any]],
                              rootGlobals: Map[String, Any],
                              defaultGlobals: Option[Map[String, Any]],
                              pipelineContext: PipelineContext,
                              merge: Boolean = false): Option[Map[String, Any]] = {
    globals.map { baseGlobals =>
      val result = baseGlobals.foldLeft(rootGlobals)((rootMap, entry) => parseValue(rootMap, entry._1, entry._2, pipelineContext))
      if (merge) {
        defaultGlobals.getOrElse(Map[String, Any]()) ++ result
      } else {
        result
      }
    }.orElse(defaultGlobals)
  }

  private def parseParameters(classInfo: ClassInfo, credentialProvider: Option[CredentialProvider], pipelineContext: PipelineContext): Map[String, Any] = {
    classInfo.parameters.getOrElse(Map[String, Any]())
      .foldLeft(Map[String, Any]("credentialProvider" -> credentialProvider))((rootMap, entry) =>
        parseValue(rootMap, entry._1, entry._2, pipelineContext))
  }

  /**
    * This function will parse the provided value and add the result to the provided rootMap using the provided key.
    *
    * @param rootMap The map to store the newly formed object.
    * @param key     The key to use when adding teh result to the rootMap
    * @param value   The value to be parsed
    * @param ctx     The PipelineContext that will provide the mapper
    * @return A map containing the converted value
    */
  def parseValue(rootMap: Map[String, Any], key: String, value: Any, ctx: PipelineContext): Map[String, Any] = {
    val jsonContext = ctx.contextManager.getContext("json").asInstanceOf[Option[Json4sContext]].get
    value match {
      case map: Map[String, Any] if map.contains("className") =>
        val mapEmbedded = map.get("mapEmbeddedVariables").exists(_.toString.toBoolean)
        val finalMap = if (mapEmbedded) {
          ctx.parameterMapper.mapEmbeddedVariables(map("object").asInstanceOf[Map[String, Any]], ctx, None)
        } else {
          map("object").asInstanceOf[Map[String, Any]]
        }
        val obj = JsonParser.parseJson(
          JsonParser.serialize(finalMap, jsonContext.serializers),
          map("className").asInstanceOf[String], jsonContext.serializers)
        rootMap + (key -> obj)
      case listMap: List[Any] =>
        val obj = listMap.map {
          case m: Map[String, Any] =>
            if (m.contains("className")) {
              val mapEmbedded = m.get("mapEmbeddedVariables").exists(_.toString.toBoolean)
              val map = if (m.contains("parameters")) {
                m("parameters").asInstanceOf[Map[String, Any]]
              } else {
                m("object").asInstanceOf[Map[String, Any]]
              }
              val finalMap = if (mapEmbedded) {
                ctx.parameterMapper.mapEmbeddedVariables(map, ctx, None)
              } else {
                map
              }
              JsonParser.parseJson(
                JsonParser.serialize(finalMap, jsonContext.serializers),
                m("className").asInstanceOf[String], jsonContext.serializers)
            } else {
              m
            }
          case any => any
        }
        rootMap + (key -> obj)
      case _ => rootMap + (key -> value)
    }
  }
}