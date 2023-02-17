package com.acxiom.metalus

import com.acxiom.metalus.applications.Json4sSerializers

import java.util.Date

/**
  * Defines the minimal attributes required for a step template and pipeline step.
  *
  * @param id              The unique (to the pipeline) id of this step. This property is used to chain steps together.
  * @param displayName     A name that can be displayed in logs and errors.
  * @param description     A long description of this step.
  * @param `type`          The type of step.
  * @param params          The step parameters that are used during execution.
  * @param engineMeta      Contains the instruction for invoking the step function.
  */
trait Step {
  def id: Option[String]
  def displayName: Option[String]
  def description: Option[String]
  def `type`: Option[String]
  def params: Option[List[Parameter]]
}

trait FlowStep extends Step {
  def nextStepId: Option[String]
  def executeIfEmpty: Option[String]
  def stepTemplateId: Option[String]
  def nextStepOnError: Option[String]
  def retryLimit: Option[Int]
  def nextSteps: Option[List[String]] = None
  def dependencies: Option[String] = None
  def value: Option[String] = None

  def valueExpression: String = value.getOrElse("STEP")

  def nextStepExpressions: Option[List[String]] = nextSteps.orElse(nextStepId.map(n => List(s"'$n'")))
}

/**
  * Metadata about the next step in the pipeline process.
  *
  * @param id              The unique (to the pipeline) id of this step. This property is used to chain steps together.
  * @param displayName     A name that can be displayed in logs and errors.
  * @param description     A long description of this step.
  * @param `type`          The type of step.
  * @param params          The step parameters that are used during execution.
  * @param engineMeta      Contains the instruction for invoking the step function.
  * @param executeIfEmpty  This field allows a value to be passed in rather than executing the step.
  * @param nextStepId      The id of the next step to execute.
  * @param stepTemplateId          The id of the step that provided the metadata.
  * @param nextStepOnError The id of the step that should be called on error
  * @param retryLimit      The number of times that this step should be retried on error. Default is -1 indicating to
  *                        not retry. This parameter will be considered before nextStepOnError.
  */
final case class PipelineStep(override val id: Option[String] = None,
                              override val displayName: Option[String] = None,
                              override val description: Option[String] = None,
                              override val `type`: Option[String] = None,
                              override val params: Option[List[Parameter]] = None,
                              override val nextStepId: Option[String] = None,
                              override val executeIfEmpty: Option[String] = None,
                              override val stepTemplateId: Option[String] = None,
                              override val nextStepOnError: Option[String] = None,
                              override val retryLimit: Option[Int] = Some(-1),
                              override val nextSteps: Option[List[String]] = None,
                              override val value: Option[String] = None,
                              override val dependencies: Option[String] = None,
                              engineMeta: Option[EngineMeta] = None) extends FlowStep

final case class PipelineStepGroup(override val id: Option[String] = None,
                                   override val displayName: Option[String] = None,
                                   override val description: Option[String] = None,
                                   override val `type`: Option[String] = None,
                                   override val params: Option[List[Parameter]] = None,
                                   override val nextStepId: Option[String] = None,
                                   override val executeIfEmpty: Option[String] = None,
                                   override val stepTemplateId: Option[String] = None,
                                   override val nextStepOnError: Option[String] = None,
                                   override val retryLimit: Option[Int] = Some(-1),
                                   override val nextSteps: Option[List[String]] = None,
                                   override val value: Option[String] = None,
                                   override val dependencies: Option[String] = None,
                                   pipelineId: Option[String] = None) extends FlowStep

/**
  * Represents a template fora step to be used when creating a pipeline.
  *
  * @param id              The unique (to the pipeline) id of this step template. This property is used to reference this template within a pipeline step.
  * @param displayName     A name that can be displayed in logs and errors.
  * @param description     A long description of this step.
  * @param `type`          The type of step.
  * @param params          The step parameters that are used during execution.
  * @param engineMeta      Contains the instruction for invoking the step function.
  * @param restartable     Boolean flag indicating whether this step may be started in a flow restart
  */
case class StepTemplate(override val id: Option[String] = None,
                        override val displayName: Option[String] = None,
                        override val description: Option[String] = None,
                        override val `type`: Option[String] = None,
                        override val params: Option[List[Parameter]] = None,
                        engineMeta: Option[EngineMeta] = None,
                        restartable: Option[Boolean] = Some(false)) extends Step

/**
  * Represents a single parameter in a step.
  *
  * @param `type`            The parameter type.
  * @param name              The parameter name. This should match the parameter name of the function being called.
  * @param required          Boolean indicating whether this parameter is required.
  * @param defaultValue      The default value to pass if the value is not set.
  * @param value             The value to be used for this parameter.
  * @param className         An optional classname used to convert a map into an object
  * @param parameterType     Contains optional type information for each parameter
  * @param description       Optional description of this parameter
  * @param json4sSerializers Optional custom json serializers
  */
case class Parameter(`type`: Option[String] = None,
                     name: Option[String] = None,
                     required: Option[Boolean] = Some(false),
                     defaultValue: Option[Any] = None,
                     value: Option[Any] = None,
                     className: Option[String] = None,
                     parameterType: Option[String] = None,
                     description: String = "",
                     json4sSerializers: Option[Json4sSerializers] = None)

/**
  * This class contains the execution information for a Step
  *
  * @param command The execution instruction for the Spark engine.
  * @param pkg An optional package location
  * @param results The optional StepResult
  */
case class EngineMeta(command: Option[String] = None, pkg: Option[String] = None, results: Option[Results] = None)

/**
  * This class represents the expected result of a step execution
  *
  * @param primaryType The expected type of the primary response
  * @param secondaryTypes An optional map of secondary response types by name
  */
case class Results(primaryType: String, secondaryTypes: Option[Map[String, String]] = None)

/**
  * Trait that defines the minimum properties required by an exception thrown by a PipelineStep
  */
trait PipelineStepException extends Exception {
  def errorType: Option[String]
  def dateTime: Option[String]
  def message: Option[String]
  def context: Option[PipelineContext]
}

/**
  * This Exception should be thrown when pipelines should stop running do to normal processing decisions. The pipeline
  * will complete all steps, but the next pipeline if any will not be executed.
  *
  * @param errorType        The type of exception. The default is pauseException
  * @param dateTime         The date and time of the exception
  * @param message          The message to detailing the reason
  * @param pipelineProgress The progress information
  * @param cause            An additional exception that may gave caused the pipeline to pause.
  */
case class PauseException(errorType: Option[String] = Some("pauseException"),
                          dateTime: Option[String] = Some(new Date().toString),
                          message: Option[String] = Some(""),
                          pipelineProgress: Option[PipelineStateKey],
                          cause: Throwable = None.orNull,
                          @transient context: Option[PipelineContext] = None)
  extends Exception(message.getOrElse(""), cause)
    with PipelineStepException

/**
  * This Exception should be thrown when pipelines should stop running do to a problem. The pipeline
  * will stop at the current step, and the next pipeline if any will not be executed.
  *
  * @param errorType        The type of exception. The default is pipelineException
  * @param dateTime         The date and time of the exception
  * @param message          The message to detailing the reason
  * @param pipelineProgress The progress information
  * @param cause            An additional exception that may gave caused the pipeline to stop.
  */
case class PipelineException(errorType: Option[String] = Some("pipelineException"),
                             dateTime: Option[String] = Some(new Date().toString),
                             message: Option[String] = Some(""),
                             pipelineProgress: Option[PipelineStateKey],
                             cause: Throwable = None.orNull,
                             @transient context: Option[PipelineContext] = None)
  extends Exception(message.getOrElse(""), cause)
    with PipelineStepException

/**
  * This Exception represents one or more exceptions that may have been received during a fork step execution.
  *
  * @param errorType  The type of exception. The default is forkStepException
  * @param dateTime   The date and time of the exception
  * @param message    The base message to detailing the reason
  * @param exceptions A list of exceptions to use when building the message
  */
case class ForkedPipelineStepException(errorType: Option[String] = Some("forkStepException"),
                                       dateTime: Option[String] = Some(new Date().toString),
                                       message: Option[String] = Some(""),
                                       exceptions: Map[Int, Throwable] = Map(),
                                       @transient context: Option[PipelineContext] = None)
  extends Exception(message.getOrElse(""))
    with PipelineStepException {
  /**
    * Adds an new exception to the internal list and returns a new ForkedPipelineStepException
    * @param t The exception to throw
    * @param executionId The id of the execution for this exception
    * @return A new ForkedPipelineStepException
    */
  def addException(t: Throwable, executionId: Int): ForkedPipelineStepException = {
    this.copy(exceptions = this.exceptions + (executionId -> t))
  }

  override def getMessage: String = {
    exceptions.foldLeft(message.get)((mess, e) => {
      s"$mess Execution ${e._1}: ${e._2.getMessage}\n"
    })
  }
}

/**
  * TODO [2.0 Review] This may no longer be needed in the new model
  * @param errorType  The type of exception. The default is forkStepException
  * @param dateTime   The date and time of the exception
  * @param message    The base message to detailing the reason
  * @param context    The PipelineContext at the time of the exception
  */
case class SkipExecutionPipelineStepException(errorType: Option[String] = Some("skipExecutionException"),
                                              dateTime: Option[String] = Some(new Date().toString),
                                              message: Option[String] = Some("Execution should be skipped"),
                                              @transient context: Option[PipelineContext] = None) extends Exception(message.getOrElse(""))
  with PipelineStepException

case class PipelineStepResponse(primaryReturn: Option[Any], namedReturns: Option[Map[String, Any]] = None)

object PipelineStepType {
  val BRANCH: String = "branch"
  val FORK: String = "fork"
  val JOIN: String = "join"
  val MERGE: String = "merge"
  val PIPELINE: String = "pipeline"
  val SPLIT: String = "split"
  val STEPGROUP: String = "step-group"
}
