package com.acxiom.pipeline

import com.acxiom.pipeline.PipelineStepMessageType.PipelineStepMessageType

import java.util.Date

/**
  * Metadata about the next step in the pipeline process.
  *
  * @param id              The unique (to the pipeline) id of this step. This pproperty is used to chain steps together.
  * @param displayName     A name that can be displayed in logs and errors.
  * @param description     A long description of this step.
  * @param `type`          The type of step.
  * @param params          The step parameters that are used during execution.
  * @param engineMeta      Contains the instruction for invoking the step function.
  * @param executeIfEmpty  This field allows a value to be passed in rather than executing the step.
  * @param nextStepId      The id of the next step to execute.
  * @param stepId          The id of the step that provided the metadata.
  * @param nextStepOnError The id of the step that should be called on error
  * @param retryLimit      The number of times that this step should be retried on error. Default is -1 indicating to
  *                        not retry. This parameter will be considered before nextStepOnError.
  */
case class PipelineStep(id: Option[String] = None,
                        displayName: Option[String] = None,
                        description: Option[String] = None,
                        `type`: Option[String] = None,
                        params: Option[List[Parameter]] = None,
                        engineMeta: Option[EngineMeta] = None,
                        nextStepId: Option[String] = None,
                        executeIfEmpty: Option[String] = None,
                        stepId: Option[String] = None,
                        nextStepOnError: Option[String] = None,
                        retryLimit: Option[Int] = Some(-1))

/**
  * Represents a single parameter in a step.
  *
  * @param `type`        The parameter type.
  * @param name          The parameter name. This should match the parameter name of the function being called.
  * @param required      Boolean indicating whether this parameter is required.
  * @param defaultValue  The default value to pass if the value is not set.
  * @param value         The value to be used for this parameter.
  * @param className     An optional classname used to convert a map into an object
  * @param parameterType Contains optional type information for each parameter
  * @param description   Optional description of this parameter
  */
case class Parameter(`type`: Option[String] = None,
                     name: Option[String] = None,
                     required: Option[Boolean] = Some(false),
                     defaultValue: Option[Any] = None,
                     value: Option[Any] = None,
                     className: Option[String] = None,
                     parameterType: Option[String] = None,
                     description: String = "")

/**
  * This class contains the execution information for a Step
  *
  * @param spark The execution instruction for the Spark engine.
  * @param pkg An optional package location
  * @param results The optional StepResult
  */
case class EngineMeta(spark: Option[String] = None, pkg: Option[String] = None, results: Option[StepResults] = None)

/**
  * This class represents the expected result of a step execution
  *
  * @param primaryType The expected type of the primary response
  * @param secondaryTypes An optional map of secondary response types by name
  */
case class StepResults(primaryType: String, secondaryTypes: Option[Map[String, String]] = None)

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
                          pipelineProgress: Option[PipelineExecutionInfo],
                          cause: Throwable = None.orNull,
                          context: Option[PipelineContext] = None)
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
                             pipelineProgress: Option[PipelineExecutionInfo],
                             cause: Throwable = None.orNull,
                             context: Option[PipelineContext] = None)
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
                                       context: Option[PipelineContext] = None)
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

trait PipelineStepMessage {
  val stepId: String
  val pipelineId: String
  val messageType: PipelineStepMessageType
  val message: String
}

object PipelineStepMessageType extends Enumeration {
  type PipelineStepMessageType = Value
  val error, warn, pause, info = Value
}

object PipelineStepMessage {
  def apply(message: String, stepId: String, pipelineId: String, messageType: PipelineStepMessageType): PipelineStepMessage =
    DefaultPipelineStepMessage(message: String, stepId, pipelineId, messageType)
}

case class DefaultPipelineStepMessage(message: String, stepId: String, pipelineId: String, messageType: PipelineStepMessageType)
  extends PipelineStepMessage

case class ForkedPipelineStepMessage(message: String,
                                     stepId: String,
                                     pipelineId: String,
                                     messageType: PipelineStepMessageType,
                                     executionId: Option[Int]) extends PipelineStepMessage

/**
  * Trait that defines the object that should be returned by a PipelineStep function.
  */
trait PipelineStepResponse {
  val primaryReturn: Option[Any]
  val namedReturns: Option[Map[String, Any]]
}

case class DefaultPipelineStepResponse(primaryReturn: Option[Any], namedReturns: Option[Map[String, Any]] = None)
  extends PipelineStepResponse

object PipelineStepResponse {
  def apply(primaryReturn: Option[Any], namedReturns: Option[Map[String, Any]]): PipelineStepResponse =
    DefaultPipelineStepResponse(primaryReturn, namedReturns)
}

object PipelineStepType {
  val BRANCH: String = "branch"
  val FORK: String = "fork"
  val JOIN: String = "join"
  val MERGE: String = "merge"
  val PIPELINE: String = "pipeline"
  val SPLIT: String = "split"
  val STEPGROUP: String = "step-group"
}
