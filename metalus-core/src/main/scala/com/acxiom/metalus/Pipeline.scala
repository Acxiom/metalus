package com.acxiom.metalus

import com.acxiom.metalus.audits.ExecutionAudit
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.parser.JsonParser

import scala.io.Source
import scala.jdk.CollectionConverters._

/**
  * Contains the a pipeline definition to be executed.
  *
  * @param id          The unique id of this pipeline.
  * @param name        The pipeline name used for logging and errors.
  * @param steps       A list of steps to execute.
  * @param tags        A list of tags to use to help describe the pipeline
  *                    the default map of step results.
  * @param description An optional description of this pipeline.
  * @param parameters  Optional parameters that may be used to control the inputts and outputs of this pipeline.
  */
case class Pipeline(id: Option[String] = None,
                    name: Option[String] = None,
                    steps: Option[List[FlowStep]] = None,
                    tags: Option[List[String]] = None,
                    description: Option[String] = None,
                    parameters: Option[Parameters] = None)

/**
  * Represents the available parameters that may be specified for a Pipeline.
  * @param inputs Optional list of parameters that must be present for this Pipeline to operate properly.
  * @param output Optional mappings that will be used to produce a PipelineStepResponse for this Pipeline.
  * @param restartableSteps List of step ids that can be restarted.
  */
final case class Parameters(inputs: Option[List[InputParameter]] = None, output: Option[PipelineResult] = None, restartableSteps: Option[List[String]] = None)

/**
  * Defines a single input parameter for a Pipeline.
  * @param name The name of the parameter.
  * @param global True if this parameter is part of the globals definition, false if it is a PipelineParameter.
  * @param required True if a value is required for this parameter.
  * @param alternates List of alternate input parameters that may be used instead of this parameter.
  */
final case class InputParameter(name: String, global: Boolean, required: Boolean = false, alternates: Option[List[String]] = None)

/**
  * Defines the result of a Pipeline.
  * @param primaryMapping The mapping string to use when setting the primary mapping of the PipelineStepResponse.
  * @param secondaryMappings A list of mappings to use for the secondary value in the PipelineStepResponse.
  */
final case class PipelineResult(primaryMapping: String, secondaryMappings: Option[List[NamedMapping]] = None)

/**
  * Provides the mapping defintion for a result.
  * @param mappedName The name of the mapping to use.
  * @param stepKey The path to the value to map.
  */
final case class NamedMapping(mappedName: String, stepKey: String)

/**
 * Represents the information used to track a fork process within an execution or pipeline
 *
 * @param index  The index of the value in the list
 * @param value  The value that is being processed
 * @param parent An optional parent fork
 */
final case class ForkData(index: Int, value: Option[Any], parent: Option[ForkData]) {
  def generateForkKeyValue(): String = {
    if (parent.isDefined) {
      s"${parent.get.generateForkKeyValue()}_$index"
    } else {
      s"$index"
    }

  }
}

object PipelineStateInfo {
  def fromString(key: String): PipelineStateInfo = {
    val keyTokens = key.split('.')
    if (keyTokens.length > 1) {
      keyTokens.foldLeft(PipelineStateInfo(""))((info, token) => {
        // See if the info is a parent
        val updatedInfo = if (info.pipelineId.nonEmpty &&
          info.stepId.isDefined &&
          !token.startsWith("f(")) {
          PipelineStateInfo("", stepGroup = Some(info))
        } else {
          info
        }

        if (token.startsWith("f(")) {
          val forkData = token.substring(2, token.indexOf(")")).split("_")
            .foldLeft(Option[ForkData](None.orNull))((fork, token) => Some(ForkData(token.toInt, None, fork)))
          updatedInfo.copy(forkData = forkData)
        } else if (updatedInfo.pipelineId.isEmpty) {
          // pipeline
          updatedInfo.copy(pipelineId = token)
        } else {
          // step
          updatedInfo.copy(stepId = Some(token))
        }
      })
    } else { // Pipeline Key
      PipelineStateInfo(key)
    }
  }
}

/**
  * Provides information about the current step being executed.
  *
  * @param pipelineId The current pipeline being executed.
  * @param stepId     Optional step being executed.
  * @param forkData   Optional information if this step is part of a fork.
  * @param stepGroup  Information about the step group which started this pipeline.
  */
final case class PipelineStateInfo(pipelineId: String,
                                   stepId: Option[String] = None,
                                   forkData: Option[ForkData] = None,
                                   stepGroup: Option[PipelineStateInfo] = None) {
  /**
    * The unique key representing this state.
    *
    * @return The state key.
    */
  lazy val key: String = {
    val key = if (stepId.isDefined) {
      s"$pipelineId.${stepId.getOrElse("")}"
    } else {
      pipelineId
    }
    val forkKey = if (forkData.isDefined) {
      s"$key.f(${forkData.get.generateForkKeyValue()})"
    } else {
      key
    }
    if (stepGroup.isEmpty) {
      forkKey
    } else {
      s"${stepGroup.get.key}.$forkKey"
    }
  }

  def displayPipelineStepString: String = {
    s"pipeline $pipelineId step ${stepId.getOrElse("")}"
  }
}

/**
  * Contains information about a class that needs to be instantiated at runtime.
  *
  * @param className The fully qualified class name.
  * @param parameters A map of simple parameters to pass to the constructor.
  */
case class ClassInfo(className: Option[String], parameters: Option[Map[String, Any]] = None)

/**
  * Global object that may be passed to step functions.
  *
  * @param globals            Contains all global objects.
  * @param parameters         The pipeline parameters being used. Contains initial parameters as well as the result
  *                           of steps that have been processed.
  * @param stepPackages       The list of packages to consider when searching for step objects.
  * @param parameterMapper    Used to map parameters to step functions
  * @param pipelineListener   Used to communicate progress through the pipeline
  * @param audits             List of audits
  * @param pipelineManager    The PipelineManager to use for Step Groups.
  * @param credentialProvider The CredentialProvider to use for accessing credentials.
  * @param contextManager     The context manager to use for accessing side loaded contexts
  * @param stepResults        A map of results from the execution of the pipeline steps.
  * @param currentStateInfo   The current pipeline state information
 *  @param restartPoints      Optional list of steps that should be restarted.
 *  @param executionEngines   An optional list of execution engines.
  */
case class PipelineContext(globals: Option[Map[String, Any]],
                           parameters: List[PipelineParameter],
                           stepPackages: Option[List[String]] = Some(List(
                             "com.acxiom.metalus",
                             "com.acxiom.metalus.steps",
                             "com.acxiom.metalus.pipeline.steps",
                             "com.acxiom.metalus",
                             "com.acxiom.metalus.steps")),
                           parameterMapper: PipelineStepMapper = PipelineStepMapper(),
                           pipelineListener: Option[PipelineListener] = Some(PipelineListener()),
                           audits: List[ExecutionAudit] = List(),
                           pipelineManager: PipelineManager = PipelineManager(List()),
                           credentialProvider: Option[CredentialProvider] = None,
                           contextManager: ContextManager,
                           stepResults: Map[PipelineStateInfo, PipelineStepResponse] = Map(),
                           currentStateInfo: Option[PipelineStateInfo] = None,
                           restartPoints: Option[RestartPoints] = None,
                           executionEngines: Option[List[String]] = Some(List("batch"))) {

  private lazy val alternateStepMaps = {
    executionEngines.getOrElse(List("batch")).filter(_ != "batch").foldLeft(List[AlternateStepMap]())((list, engine) => {
      val stream = getClass.getResourceAsStream(s"/metadata/steps/$engine-mappings.json")
      if (Option(stream).isDefined) {
        list :+
          JsonParser.parseJson(
            Source.fromInputStream(stream).mkString,
            "com.acxiom.metalus.AlternateStepMap").asInstanceOf[AlternateStepMap]
      } else {
        list
      }
    })
  }

  /**
   * This method will see if there is an alternate step command that should be used.
   * @param stepTemplateId The step template id being referenced.
   * @return Either an alternate command or None if there is no alternative.
   */
  def getAlternateCommand(stepTemplateId: String): Option[String] = {
    val map = alternateStepMaps.find(map => map.alternatives.exists(_.stepId == stepTemplateId))
    if (map.isDefined) {
      val step = map.get.alternatives.find(_.stepId == stepTemplateId)
      if (step.isDefined) {
        val command = map.get.alternativeStepCommands.find(_.stepId == step.get.alternativeStepId)
        if (command.isDefined) {
          Some(command.get.command)
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
    * Merges the provided PipelineContext audits, stepResults and globals onto this PipelineContext. Merge is additive
    * unless the value already exists in which case, the provide PipelineContext value will be used.
    * @param pipelineContext The PipelineContext to merge.
    * @return A modified PipelineContext
    */
  def merge(pipelineContext: PipelineContext): PipelineContext = {
    val newResults = pipelineContext.stepResults
      .foldLeft((this.stepResults, this))((results, result) => {
      if (this.stepResults.keys.exists(_.key == result._1.key)) {
        // Replace the current result with the incoming result
        (results._1 + (result._1 -> result._2), results._2)
      } else {
        // See if there may be global updates
        val ctx = if (result._2.namedReturns.isDefined) {
          result._2.namedReturns.get.foldLeft(results._2)((updateCtx, entry) => {
            if (entry._1.startsWith("$globals.")) {
              updateCtx.setGlobal(entry._1.substring(Constants.NINE), entry._2)
            } else if (entry._1.startsWith("$globalLink.")) {
              updateCtx.setGlobalLink(entry._1.substring(Constants.TWELVE), entry._2.toString)
            } else {
              updateCtx
            }
          })
        } else {
          results._2
        }
        (results._1 + result, ctx)
      }
    })

    val newAudits = pipelineContext.audits.foldLeft(this.audits)((audits, audit) => {
      val index = this.audits.indexWhere(_.key.key == audit.key.key)
      if (index != -1) {
        audits.updated(index, audits(index).merge(audit))
      } else {
        audits :+ audit
      }
    })

    val restartMerge = if (this.restartPoints.isEmpty || pipelineContext.restartPoints.isEmpty) {
      None
    } else {
      this.restartPoints
    }

    newResults._2.copy(stepResults = newResults._1, audits = newAudits, restartPoints = restartMerge)
  }

  /**
    * Get the named global value as a string.
    *
    * @param globalName The name of the global property to return.
    * @return An option containing the value or None
    */
  def getGlobalString(globalName: String): Option[String] = {
    getGlobal(globalName).flatMap{
      case s: String => Some(s)
      case _ => None
    }
  }

  /**
    * Get the named global value (considering GlobalLinks)
    *
    * @param globalName The name of the global property to return.
    * @return An option containing the value or None
    */
  def getGlobal(globalName: String): Option[Any] = {
    this.globals.flatMap(x => {
      x.collectFirst{
        case ("GlobalLinks", v:Map[_,_]) if v.isInstanceOf[Map[String, Any]] && v.asInstanceOf[Map[String, Any]].contains(globalName) =>
          v.asInstanceOf[Map[String, Any]].getOrElse(globalName, "")
        case (k, v:Some[_]) if k == globalName => v.get
        case (k, v) if k == globalName && !v.isInstanceOf[Option[_]] => v
      }
    })
  }

  /**
   * Get the named global value (considering GlobalLinks) and perform a cast
   *
   * @param globalName The name of the global property to return.
   * @return An option containing the value cast to the specified type or None
   */
  def getGlobalAs[T](globalName: String): Option[T] = {
    getGlobal(globalName).map(_.asInstanceOf[T])
  }

  /**
   * Determines if the named global is a global link.
   * @param globalName The name of the global property to check.
   * @return true if the named global is a global link.
   */
  def isGlobalLink(globalName: String): Boolean = {
    val links = getGlobalAs[Map[String, Any]]("GlobalLinks")
    links.getOrElse(Map[String, Any]()).asJava.containsKey(globalName)
  }

  /**
   * This function will add or update a single entry on the globals map.
   *
   * @param globalName  The name of the global property to set.
   * @param globalValue The value of the global property to set.
   * @return A new PipelineContext with an updated globals map.
   */
  def setGlobal(globalName: String, globalValue: java.io.Serializable): PipelineContext =
    this.copy(globals = Some(this.globals.getOrElse(Map[String, Any]()) + (globalName -> globalValue)))

  /**
   * This function will add or update a single entry on the globals map.
   *
   * @param globalName  The name of the global property to set.
   * @param globalValue The value of the global property to set.
   * @return A new PipelineContext with an updated globals map.
   */
  def setGlobal(globalName: String, globalValue: Any): PipelineContext =
    this.copy(globals = Some(this.globals.getOrElse(Map[String, Any]()) + (globalName -> globalValue)))

  /**
   * This function will merge an existing Map[String, Any] into the globals map
   *
   * @param globals A Map[String, Any] of global properties.
   * @return A new PipelineContext with an updated globals map.
   */
  def setGlobals(globals: Map[String, Any]): PipelineContext =
    this.copy(globals = Some(if (this.globals.isDefined) this.globals.get ++ globals else globals))

  /**
   * This function will add or update a single GlobalLink.
   *
   * @param name The name of the GlobalLink
   * @param path Thee path to be evaluated
   * @return A new PipelineContext with an updated GlobalLinks map.
   */
  def setGlobalLink(name: String, path: String): PipelineContext = {
    val links = getGlobalAs[Map[String, Any]]("GlobalLinks").getOrElse(Map()) + (name -> path)
    val newGlobals: Map[String, Any] = if (this.globals.isDefined) {
      this.globals.get + ("GlobalLinks" -> links)
    } else {
      Map("GlobalLinks" -> links)
    }
    this.copy(globals = Some(newGlobals))
  }

  /**
   * This function will remove a single entry on the globals map.
   *
   * @param globalName The name of the global property to remove.
   * @return A new PipelineContext with an updated globals map.
   */
  def removeGlobal(globalName: String): PipelineContext =
    this.copy(globals = Some(this.globals.getOrElse(Map[String, Any]()) - globalName))

  /**
    * Returns the result of an executed step or None if the step response does not exist.
    * @param stepKey The unique step key.
    * @return The response value or None if no value exists.
    */
  def getStepResultByKey(stepKey: String): Option[PipelineStepResponse] = {
    val result = stepResults.find(key => key._1.key == stepKey)
    if (result.isDefined) {
      Some(result.get._2)
    } else {
      None
    }
  }

  /**
   * Returns a list of results. This method is useful when a step was executed within a fork.
   *
   * @param stepKey The unique step key.
   * @return The response value or None if no value exists.
   */
  def getStepResultsByKey(stepKey: String): Option[List[PipelineStepResponse]] = {
    val defaultForkData = ForkData(0, None, None)
    val filteredResponses = this.stepResults.filter(_._1.copy(forkData = None).key == stepKey)
    val sortedKeys = filteredResponses.keys.toList
      .sortWith(_.forkData.getOrElse(defaultForkData).index < _.forkData.getOrElse(defaultForkData).index)
    val responses = sortedKeys.map(k => filteredResponses(k))
    if (responses.nonEmpty) {
      Some(responses)
    } else {
      None
    }
  }

  /**
    * Returns the result of an executed step or None if the step response does not exist.
    * @param stepKey The unique step key.
    * @return The response value or None if no value exists.
    */
  def getStepResultByStateInfo(stepKey: PipelineStateInfo): Option[PipelineStepResponse] = getStepResultByKey(stepKey.key)

  /**
   * Returns a list of response objects for the named step. This method is primarily useful when a step
   * executes within a fork. The results will be returned in index order of the fork information.
   * @param stepKey The lookup key.
   * @return A list of responses or None.
   */
  def getStepResultsByStateInfo(stepKey: PipelineStateInfo): Option[List[PipelineStepResponse]] =
    getStepResultsByKey(stepKey.copy(forkData = None).key)

  /**
   * This function provides a short cut for adding values to the pipeline parameters object.
   *
   * @param stepKey The unique step key.
   * @param result  The result of the step execution.
   * @return An updated PipelineContext.
   */
  def setPipelineStepResponse(stepKey: PipelineStateInfo, result: PipelineStepResponse): PipelineContext =
    this.copy(stepResults = stepResults + (stepKey -> result))

  /**
    * This method will locate the pipeline parameter for the provided key.
    *
    * @param pipelineKey The unique pipeline key.
    * @return The pipeline parameter if it exists.
    */
  def findParameterByPipelineKey(pipelineKey: String): Option[PipelineParameter] =
    parameters.find(p => p.pipelineKey.key == pipelineKey)

  /**
   * This method provides a shortcut for getting values from the pipeline parameters object.
   *
   * @param pipelineKey The unique pipeline key.
   * @param name The name of the parameter to get.
   * @return The pipeline parameter value, if it exists.
   */
  def getParameterByPipelineKey(pipelineKey: String, name: String): Option[Any] =
    findParameterByPipelineKey(pipelineKey).flatMap(_.parameters.get(name))

  /**
    * This method will add a new parameter lookup to the PipelineParameter specified by the key. If none exists,
    * a new parameter will be added.
    * @param pipelineKey The unique for the Pipeline
    * @param name The name of the parameter to add.
    * @param value The value to be added.
    * @return An updated PipelineContext
    */
  def setPipelineParameterByKey(pipelineKey: PipelineStateInfo, name: String, value: Any): PipelineContext = {
    val index = this.parameters.indexWhere(_.pipelineKey.key == pipelineKey.key)
    val parameter = if (index == -1) {
      PipelineParameter(pipelineKey, Map())
    } else {
      this.parameters(index)
    }
    val updatedParameter = parameter.copy(parameters = parameter.parameters + (name -> value))
    val updatedParameters = if (index > -1) {
      this.parameters.updated(index, updatedParameter)
    } else {
      this.parameters :+ updatedParameter
    }
    this.copy(parameters = updatedParameters)
  }

  /**
    * This will determine if the pipeline parameters contains anything for the given pipelineId.
    *
    * @param pipelineKey The unique pipeline
   *  @param paramName The name of the parameter to check.
    * @return true if the pipeline parameters has something for this id.
    */
  def hasPipelineParameters(pipelineKey: String, paramName: String): Boolean =
    parameters.exists(p => p.pipelineKey.key == pipelineKey && p.parameters.contains(paramName))

  /**
    * This will determine if the pipeline parameters contains anything for the given pipelineId.
    *
    * @param pipelineKey The unique pipeline
    * @return true if the pipeline parameters has something for this id.
    */
  def hasPipelineParameters(pipelineKey: PipelineStateInfo): Boolean =
    parameters.exists(_.pipelineKey.key == pipelineKey.copy(stepId = None, forkData = None).key)

  def hasAudit(pipelineKey: PipelineStateInfo): Boolean = audits.exists(_.key.key == pipelineKey.key)

  /**
    * Sets the current state info on the context.
    * @param pipelineStateInfo The new state info.
    * @return Updated PipelineContext
    */
  def setCurrentStateInfo(pipelineStateInfo: PipelineStateInfo): PipelineContext =
    this.copy(currentStateInfo = Some(pipelineStateInfo))

  /**
    * Add an audit for this pipeline context
    *
    * @param audit The audit to make root
    * @return
    */
  def setPipelineAudit(audit: ExecutionAudit): PipelineContext = {
    if (this.hasAudit(audit.key)) {
      val index = audits.indexWhere(_.key.key == audit.key.key)
      this.copy(audits = audits.updated(index, audit))
    } else {
      this.copy(audits = audits :+ audit)
    }
  }

  /**
    * Add or replace a metric value on an audit
    *
    * @param name The name of the metric to add or replace
    * @param value The value of the metric entry
    * @return An updated pipeline context with the metric
    */
  def setPipelineAuditMetric(auditKey: PipelineStateInfo, name: String, value: Any): PipelineContext = {
    val audit = this.getPipelineAudit(auditKey)
    val updatedAudit = audit.get.setMetric(name, value)
    val index = audits.indexWhere(_.key.key == auditKey.key)
    this.copy(audits = audits.updated(index, updatedAudit))
  }

  /**
    * Retrieves the audit for the specified pipeline key
    *
    * @param auditKey The audit key to retrieve
    * @return An audit or None
    */
  def getPipelineAudit(auditKey: PipelineStateInfo): Option[ExecutionAudit] =
    this.audits.find(a => {
      a.key.key == auditKey.key
    })

  /**
    * Retrieves the audit for the specified pipeline key. This method will remove the forkData to return
    * all audits for a named step.
    *
    * @param auditKey The audit key to retrieve
    * @return An audit or None
    */
  def getPipelineAudits(auditKey: PipelineStateInfo): Option[List[ExecutionAudit]] =
    Some(this.audits.filter(_.key.copy(forkData = None).key == auditKey.copy(forkData = None).key))
}

case class PipelineParameter(pipelineKey: PipelineStateInfo, parameters: Map[String, Any])

case class AlternateStepMap(alternatives: List[StepMap], alternativeStepCommands: List[StepCommand])

case class StepMap(stepId: String, alternativeStepId: String)

case class StepCommand(stepId: String, command: String)

/**
 * Contains a list of step kes and the action that may be taken.
 *
 * @param steps The list of keys and status.
 */
case class RestartPoints(steps: List[StepState])

/**
 * Contains the key for the step and the status: RESTART or COMPLETE
 *
 * @param key    The unique key for the step.
 * @param status The action to take on this step: RESTART or COMPLETE
 */
case class StepState(key: PipelineStateInfo, status: String)

/**
  * This class represents the result of executing a list of pipelines.
  *
  * @param pipelineContext The final pipeline context when execution stopped
  * @param success         Boolean flag indicating whether pipelines ran to completion (true) or stopped due to an error or message (false)
  * @param paused          Flag indicating whether the "failure" was actually a pause
  * @param exception       The original exception
  */
case class PipelineExecutionResult(pipelineContext: PipelineContext,
                                   success: Boolean,
                                   paused: Boolean,
                                   exception: Option[Throwable])
