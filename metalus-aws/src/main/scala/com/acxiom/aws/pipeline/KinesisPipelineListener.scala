package com.acxiom.aws.pipeline

import com.acxiom.aws.utils.{AWSUtilities, KinesisUtilities}
import com.acxiom.pipeline._

class KinesisPipelineListener(val key: String,
                              val credentialName: String,
                              val credentialProvider: CredentialProvider,
                              region: String,
                              streamName: String,
                              partitionKey: String) extends EventBasedPipelineListener {
  private lazy val awsCredential = AWSUtilities.getAWSCredential(Some(credentialProvider), credentialName)
  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generateExecutionMessage("executionStarted", pipelines),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generateExecutionMessage("executionFinished", pipelines),
      region, streamName, partitionKey, awsCredential)
    KinesisUtilities.postMessageWithCredentials(generateAuditMessage("executionFinishedAudit", pipelineContext.rootAudit),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    KinesisUtilities.postMessageWithCredentials(generateExecutionMessage("executionStopped", pipelines),
      region, streamName, partitionKey, awsCredential)
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generatePipelineMessage("pipelineStarted", pipeline),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generatePipelineMessage("pipelineFinished", pipeline),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generatePipelineStepMessage("pipelineStepStarted", pipeline, step, pipelineContext),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generatePipelineStepMessage("pipelineStepFinished", pipeline, step, pipelineContext),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    KinesisUtilities.postMessageWithCredentials(generateExceptionMessage("pipelineStepFinished", exception, pipelineContext),
      region, streamName, partitionKey, awsCredential)
  }
}
