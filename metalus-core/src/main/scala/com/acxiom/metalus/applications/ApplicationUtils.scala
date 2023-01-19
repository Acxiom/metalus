package com.acxiom.metalus.applications

import com.acxiom.metalus._
import com.acxiom.metalus.context.Json4sContext
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.utils.ReflectionUtils
import org.slf4j.LoggerFactory

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
    val validateArgumentTypes = parameters.getOrElse(Map()).getOrElse("validateStepParameterTypes", false).asInstanceOf[Boolean]
    // Create the ContextManager
    val contextManager = new ContextManager(application.contexts.getOrElse(Map()), parameters.getOrElse(Map()))
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
      globalStepMapper.get, globalListener, List(), pipelineManager, credentialProvider, contextManager, Map(), None)

    val defaultGlobals = generateGlobals(application.globals, rootGlobals , Some(rootGlobals), initialContext)
    initialContext.copy(globals = defaultGlobals)
  }

  /** TODO [2.0 Review] IS this still needed?
   * Utility method that resets the state on the PipelineExecution.
   *
   * @param application       The Application configuration
   * @param rootGlobals       The initial set of globals
   * @param execution         The execution configuration
   * @param pipelineExecution The PipelineExecution that needs to be refreshed
   * @return An updated PipelineExecution
   */
//  def refreshPipelineExecution(application: Application,
//                               rootGlobals: Option[Map[String, Any]],
//                               execution: Execution,
//                               pipelineExecution: PipelineExecution): PipelineExecution = {
//    implicit val formats: Formats = getJson4sFormats(application.json4sSerializers)
//    val initialContext = pipelineExecution.pipelineContext.copy(globals = rootGlobals)
//    val defaultGlobals = generateGlobals(application.globals, rootGlobals.get, rootGlobals, initialContext)
//    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(PipelineParameters()))
//    val ctx = pipelineExecution.pipelineContext
//      .copy(globals = generateGlobals(execution.globals, rootGlobals.get, defaultGlobals,
//        initialContext, execution.mergeGlobals.getOrElse(false)))
//      .copy(parameters = generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get)
//    pipelineExecution.asInstanceOf[DefaultPipelineExecution].copy(pipelineContext = ctx)
//  }

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
