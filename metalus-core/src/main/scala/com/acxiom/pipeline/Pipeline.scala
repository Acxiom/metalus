package com.acxiom.pipeline

import com.acxiom.pipeline.ExecutionEvaluationResult.ExecutionEvaluationResult
import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CollectionAccumulator
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConversions._

/**
  * This object provides an easy way to create a new Pipeline.
  */
object Pipeline {
  def apply(id: Option[String] = None,
            name: Option[String] = None,
            steps: Option[List[PipelineStep]] = None,
            category: Option[String] = Some("pipeline")): Pipeline = DefaultPipeline(id, name, steps, category)
}

trait Pipeline {
  def id: Option[String] = None
  def name: Option[String] = None
  def steps: Option[List[PipelineStep]] = None
  def category: Option[String] = Some("pipeline")
  def tags: Option[List[String]] = None
  def stepGroupResult: Option[Any] = None
}

/**
  * Contains the a pipeline definition to be executed.
  *
  * @param id              The unique id of this pipeline.
  * @param name            The pipeline name used for logging and errors.
  * @param steps           A list of steps to execute.
  * @param category        The category of pipeline: pipeline or step-group
  * @param tags            A list of tags to use to help describe the pipeline
  * @param stepGroupResult A mapping used to provide a single result after a step-group has executed instead of
  *                        the default map of step results.
  */
case class DefaultPipeline(override val id: Option[String] = None,
                           override val name: Option[String] = None,
                           override val steps: Option[List[PipelineStep]] = None,
                           override val category: Option[String] = Some("pipeline"),
                           override val tags: Option[List[String]] = None,
                           override val stepGroupResult: Option[Any] = None) extends Pipeline

/**
  * Global object that may be passed to step functions.
  *
  * @param sparkConf          The Spark Configuration Object.
  * @param sparkSession       The Spark Session Object.
  * @param globals            Contains all global objects.
  * @param security           The PipelineSecurityManager to use when processing steps
  * @param parameters         The pipeline parameters being used. Contains initial parameters as well as the result
  *                           of steps that have been processed.
  * @param stepPackages       The list of packages to consider when searching for step objects.
  * @param parameterMapper    Used to map parameters to step functions
  * @param pipelineListener   Used to communicate progress through the pipeline
  * @param stepMessages       Used for logging messages from steps.
  * @param rootAudit          The base audit record
  * @param pipelineManager    The PipelineManager to use for Step Groups.
  * @param credentialProvider The CredentialProvider to use for accessing credentials.
  * @param json4sFormats      The json4s Formats used when serializing/deserializing json.
  */
case class PipelineContext(sparkConf: Option[SparkConf] = None,
                           sparkSession: Option[SparkSession] = None,
                           globals: Option[Map[String, Any]],
                           security: PipelineSecurityManager = PipelineSecurityManager(),
                           parameters: PipelineParameters,
                           stepPackages: Option[List[String]] = Some(List("com.acxiom.pipeline", "com.acxiom.pipeline.steps")),
                           parameterMapper: PipelineStepMapper = PipelineStepMapper(),
                           pipelineListener: Option[PipelineListener] = Some(PipelineListener()),
                           stepMessages: Option[CollectionAccumulator[PipelineStepMessage]],
                           rootAudit: ExecutionAudit = ExecutionAudit("root", AuditType.EXECUTION, Map[String, Any](), System.currentTimeMillis()),
                           pipelineManager: PipelineManager = PipelineManager(List()),
                           credentialProvider: Option[CredentialProvider] = None,
                           json4sFormats: Option[Formats] = None) {
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

  def getGlobalAs[T](globalName: String): Option[T] = {
    getGlobal(globalName).map(_.asInstanceOf[T])
  }

  def isGlobalLink(globalName: String): Boolean = {
    val links = getGlobalAs[Map[String, Any]]("GlobalLinks")
    links.getOrElse(Map[String, Any]()).containsKey(globalName)
  }

  /**
   * This method provides a shortcut for getting values from the pipeline parameters object.
   *
   * @param pipelineId The id of the pipeline.
   * @param name The name of the parameter to set.
   * @return The pipeline parameter value, if it exists.
   */
  def getParameterByPipelineId(pipelineId: String, name: String): Option[Any] = {
    parameters.getParametersByPipelineId(pipelineId).flatMap(_.parameters.get(name))
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
  def setGlobal(globalName: String, globalValue: Serializable): PipelineContext =
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
    this.copy(globals = Some(if(this.globals.isDefined) this.globals.get ++ globals else globals))

  /**
    * This function will remove a single entry on the globals map.
    *
    * @param globalName The name of the global property to remove.
    * @return A new PipelineContext with an updated globals map.
    */
  def removeGlobal(globalName: String): PipelineContext =
    this.copy(globals = Some(this.globals.getOrElse(Map[String, Any]()) - globalName))

  /**
    * Adds a new PipelineStepMessage to the context
    *
    * @param message The message to add.
    */
  def addStepMessage(message: PipelineStepMessage): Unit = {
    if (stepMessages.isDefined) stepMessages.get.add(message)
  }

  /**
    * Returns a list of PipelineStepMessages.
    *
    * @return a list of PipelineStepMessages
    */
  def getStepMessages: Option[List[PipelineStepMessage]] = {
    if (stepMessages.isDefined) {
      Some(stepMessages.get.value.toList)
    } else {
      None
    }
  }

  /**
    * This function provides a short cut for adding values to the pipeline parameters object.
    *
    * @param pipelineId The id of the pipeline.
    * @param name The name of the parameter to set.
    * @param parameter The value of the parameter to set.
    * @return An updated PipelineContext.
    */
  def setParameterByPipelineId(pipelineId: String, name: String, parameter: Any): PipelineContext = {
    val params = parameters.setParameterByPipelineId(pipelineId, name, parameter)
    this.copy(parameters = params)
  }

  /**
    * Sets the root audit for this pipeline context
    *
    * @param audit The audit to make root
    * @return
    */
  def setRootAudit(audit: ExecutionAudit): PipelineContext = {
    this.copy(rootAudit = audit)
  }

  /**
    * Add or replace a metric value on the root audit
    *
    * @param name The name of the metric to add or replace
    * @param value The value of the metric entry
    * @return An updated pipeline context with the metric
    */
  def setRootAuditMetric(name: String, value: Any): PipelineContext = {
    val audit = this.rootAudit.setMetric(name, value)
    this.copy(rootAudit = audit)
  }

  /**
    * Retrieves the audit for the specified pipeline id
    *
    * @param id The pipeline id to retrieve
    * @return An audit or None
    */
  def getPipelineAudit(id: String): Option[ExecutionAudit] = {
    this.rootAudit.getChildAudit(id, None)
  }

  /**
    * Adds the provided pipeline audit to the root audit. Any existing audit will be overwritten.
    *
    * @param audit The audit to add
    * @return An updated PipelineContext
    */
  def setPipelineAudit(audit: ExecutionAudit): PipelineContext = {
    this.copy(rootAudit = this.rootAudit.setChildAudit(audit))
  }

  /**
    * Add or replace a metric value on the pipeline audit
    *
    * @param id The id of the pipeline
    * @param name The name of the metric to add or replace
    * @param value The value of the metric entry
    * @return An updated pipeline context with the metric
    */
  def setPipelineAuditMetric(id: String, name: String, value: Any): PipelineContext = {
    val audit = this.rootAudit.getChildAudit(id, None)
      .getOrElse(ExecutionAudit(id, AuditType.STEP, Map[String, Any](), System.currentTimeMillis()))
      .setMetric(name, value)
    this.copy(rootAudit = this.rootAudit.setChildAudit(audit))
  }

  /**
    * Returns a step audit for the provided pipeline
    *
    * @param pipelineId The id of the pipeline
    * @param stepId The main id of the audit
    * @param groupId The optional group id of the audit
    * @return The step audit or None
    */
  def getStepAudit(pipelineId: String, stepId: String, groupId: Option[String]): Option[ExecutionAudit] = {
    this.rootAudit.getChildAudit(pipelineId).get.getChildAudit(stepId, groupId)
  }

  /**
    * Set the provided audit as a child of the specified pipeline audit. This audit will replace any existing audit.
    *
    * @param pipelineId The pipeline id to add this audit to
    * @param audit The audit to set.
    * @return An updated PipelineContext
    */
  def setStepAudit(pipelineId: String, audit: ExecutionAudit): PipelineContext = {
    this.copy(rootAudit = this.rootAudit.setChildAudit(getPipelineAudit(pipelineId).get.setChildAudit(audit)))
  }

  /**
    * Add or replace a metric value on the step audit
    *
    * @param pipelineId The id of the pipeline
    * @param stepId     The main id of the audit
    * @param groupId The optional group id of the audit
    * @param name       The name of the metric to add or replace
    * @param value      The value of the metric entry
    * @return An updated pipeline context with the metric
    */
  def setStepMetric(pipelineId: String, stepId: String, groupId: Option[String], name: String, value: Any): PipelineContext = {
    val audit = getStepAudit(pipelineId, stepId, groupId)
      .getOrElse(ExecutionAudit(stepId, AuditType.STEP, Map[String, Any](), System.currentTimeMillis(), groupId=groupId))
      .setMetric(name, value)
    this.setStepAudit(pipelineId, audit)
  }

  def getPipelineExecutionInfo: PipelineExecutionInfo = {
    PipelineExecutionInfo(
      getGlobalString("stepId"),
      getGlobalString("pipelineId"),
      getGlobalString("executionId"),
      getGlobalString("groupId")
    )
  }

  /**
   * Returns current json4s Formats on the pipeline context, or DefaultFormats
   * @return A json4s Formats object.
   */
  def getJson4sFormats: Formats = json4sFormats.getOrElse(DefaultFormats)
}

case class PipelineParameter(pipelineId: String, parameters: Map[String, Any])

/**
  * Represents initial parameters for each pipeline as well as results from step execution.
  *
  * @param parameters An initial list of pipeline parameters
  */
case class PipelineParameters(parameters: List[PipelineParameter] = List()) {
  /**
    * Returns the PipelineParameter for the given pipeline id.
    *
    * @param pipelineId The id of the pipeline
    * @return An Option containing the parameter
    */
  def getParametersByPipelineId(pipelineId: String): Option[PipelineParameter] =
    parameters.find(p => p.pipelineId == pipelineId)

  /**
    * This will set a named parameter on for the provided pipeline id.
    *
    * @param pipelineId The id of the pipeline
    * @param name       The name of the parameter
    * @param parameter  The parameter value
    * @return A new copy of PipelineParameters
    */
  def setParameterByPipelineId(pipelineId: String, name: String, parameter: Any): PipelineParameters = {
    val param = getParametersByPipelineId(pipelineId)
    val updatedParameters = if (param.isDefined) {
      val p = param.get.copy(parameters = param.get.parameters + (name -> parameter))
      parameters.map(ps => if (ps.pipelineId == pipelineId) p else ps)
    } else {
      parameters :+ PipelineParameter(pipelineId, Map[String, Any](name -> parameter))
    }
    this.copy(parameters = updatedParameters)
  }

  /**
    * This will determine if the pipeline parameters contains anything for the given pipelineId.
    *
    * @param pipelineId Te id to verify.
    * @return true if the pipeline parameters has something for this id.
    */
  def hasPipelineParameters(pipelineId: String): Boolean = {
    getParametersByPipelineId(pipelineId).isDefined
  }
}

/**
  * This class represents the result of executing a list of pipelines.
  *
  * @param pipelineContext The final pipeline context when execution stopped
  * @param success         Boolean flag indicating whether pipelines ran to completion (true) or stopped due to an error or message (false)
  * @param paused          Flag indicating whether the "failure" was actually a pause
  * @param exception       The original exception
  * @param runStatus       Status indicating whether the calling execution should continue execution.
  */
case class PipelineExecutionResult(pipelineContext: PipelineContext,
                                   success: Boolean,
                                   paused: Boolean,
                                   exception: Option[Throwable],
                                   runStatus: ExecutionEvaluationResult = ExecutionEvaluationResult.RUN)

object ExecutionEvaluationResult extends Enumeration {
  type ExecutionEvaluationResult = Value
  val RUN, SKIP, STOP = Value
}

/**
  * Contains the current pipeline and step information
  *
  * @param stepId      The current step being executed
  * @param pipelineId  The current pipeline being executed
  * @param executionId The current execution being executed
  * @param groupId     The current group being executed
  */
case class PipelineExecutionInfo(stepId: Option[String] = None,
                                 pipelineId: Option[String] = None,
                                 executionId: Option[String] = None,
                                 groupId: Option[String] = None) {
  def displayPipelineStepString: String = {
    s"pipeline ${pipelineId.getOrElse("")} step ${stepId.getOrElse("")}"
  }

  def displayString: String = {
    s"execution ${executionId.getOrElse("")} group ${groupId.getOrElse("")} pipeline ${pipelineId.getOrElse("")} step ${stepId.getOrElse("")}"
  }
}
