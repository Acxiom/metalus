package com.acxiom.pipeline

import java.util.Date

import com.acxiom.pipeline.PipelineStepMessageType.PipelineStepMessageType

/**
  * Metadata about the next step in the pipeline process.
  *
  * @param id             The unique (to the pipeline) id of this step. This pproperty is used to chain steps together.
  * @param displayName    A name that can be displayed in logs and errors.
  * @param description    A long description of this step.
  * @param `type`         The type of step.
  * @param params         The step parameters that are used during execution.
  * @param engineMeta     Contains the instruction for invoking the step function.
  * @param executeIfEmpty This field allows a value to be passed in rather than executing the step.
  * @param nextStepId     The id of the next step to execute.
  */
case class PipelineStep(id: Option[String] = None,
                        displayName: Option[String] = None,
                        description: Option[String] = None,
                        `type`: Option[String] = None,
                        params: Option[List[Parameter]] = None,
                        engineMeta: Option[EngineMeta] = None,
                        nextStepId: Option[String] = None,
                        executeIfEmpty: Option[String] = None)

/**
  * Represents a single parameter in a step.
  *
  * @param `type`       The parameter type.
  * @param name         The parameter name. This should match the parameter name of the function being called.
  * @param required     Boolean indicating whether this parameter is required.
  * @param defaultValue The default value to pass if the value is not set.
  * @param value        The value to be used for this parameter.
  */
case class Parameter(`type`: Option[String] = None,
                     name: Option[String] = None,
                     required: Option[Boolean] = Some(false),
                     defaultValue: Option[Any] = None,
                     value: Option[Any] = None,
                     className: Option[String] = None)

/**
  * This class contains the execution information for a Step
  *
  * @param spark The execution instruction for the Spark engine.
  */
case class EngineMeta(spark: Option[String] = None)

/**
  * Trait that defines the minimum properties required by an exception thrown by a PipelineStep
  */
trait PipelineStepException extends Exception {
  def errorType: Option[String]
  def dateTime: Option[String]
  def message: Option[String]
}

/**
  * This Exception should be thrown when pipelines should stop running do to normal processing decisions. The pipeline
  * will complete all steps, but the next pipeline if any will not be executed.
  *
  * @param errorType  The type of exception. The default is pauseException
  * @param dateTime   The date and time of the exception
  * @param message    The message to detailing the reason
  * @param pipelineId The id of the pipeline being executed
  * @param stepId     The step being executed
  * @param cause      An additional exception that may gave caused the pipeline to pause.
  */
case class PauseException(errorType: Option[String] = Some("pauseException"),
                          dateTime: Option[String] = Some(new Date().toString),
                          message: Option[String] = Some(""),
                          pipelineId: Option[String],
                          stepId: Option[String],
                          cause: Throwable = None.orNull)
  extends Exception(message.getOrElse(""), cause)
    with PipelineStepException

/**
  * This Exception should be thrown when pipelines should stop running do to a problem. The pipeline
  * will stop at the current step, and the next pipeline if any will not be executed.
  *
  * @param errorType  The type of exception. The default is pipelineException
  * @param dateTime   The date and time of the exception
  * @param message    The message to detailing the reason
  * @param pipelineId The id of the pipeline being executed
  * @param stepId     The step being executed
  * @param cause      An additional exception that may gave caused the pipeline to stop.
  */
case class PipelineException(errorType: Option[String] = Some("pipelineException"),
                             dateTime: Option[String] = Some(new Date().toString),
                             message: Option[String] = Some(""),
                             pipelineId: Option[String],
                             stepId: Option[String],
                             cause: Throwable = None.orNull)
  extends Exception(message.getOrElse(""), cause)
    with PipelineStepException

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
