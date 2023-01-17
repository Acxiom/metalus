package com.acxiom.metalus.spark

import com.acxiom.metalus.spark.PipelineStepMessageType.PipelineStepMessageType

trait PipelineStepMessage {
  val stepId: String
  val pipelineId: String
  val messageType: PipelineStepMessageType
  val message: String
}

object PipelineStepMessageType extends Enumeration {
  type PipelineStepMessageType = Value
  val error, warn, pause, info, skip = Value
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
