package com.acxiom.aws.pipeline

import com.acxiom.aws.utils.{AWSUtilities, KinesisUtilities}
import com.acxiom.pipeline._

class KinesisPipelineListener(val key: String,
                              val credentialName: String,
                              val credentialProvider: CredentialProvider,
                              region: String,
                              streamName: String,
                              partitionKey: String) extends EventBasedPipelineListener {
  private lazy val (accessKey, secretKey) = AWSUtilities.getAWSCredentials(Some(credentialProvider), credentialName)
  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessage(generateExecutionMessage("executionStarted", pipelines),
      region, streamName, partitionKey, accessKey, secretKey)
    Some(pipelineContext)
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessage(generateExecutionMessage("executionFinished", pipelines),
      region, streamName, partitionKey, accessKey, secretKey)
    KinesisUtilities.postMessage(generateAuditMessage("executionFinishedAudit", pipelineContext.rootAudit),
      region, streamName, partitionKey, accessKey, secretKey)
    Some(pipelineContext)
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    KinesisUtilities.postMessage(generateExecutionMessage("executionStopped", pipelines),
      region, streamName, partitionKey, accessKey, secretKey)
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessage(generatePipelineMessage("pipelineStarted", pipeline),
      region, streamName, partitionKey, accessKey, secretKey)
    Some(pipelineContext)
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessage(generatePipelineMessage("pipelineFinished", pipeline),
      region, streamName, partitionKey, accessKey, secretKey)
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessage(generatePipelineStepMessage("pipelineStepStarted", pipeline, step, pipelineContext),
      region, streamName, partitionKey, accessKey, secretKey)
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessage(generatePipelineStepMessage("pipelineStepFinished", pipeline, step, pipelineContext),
      region, streamName, partitionKey, accessKey, secretKey)
    Some(pipelineContext)
  }

  override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    KinesisUtilities.postMessage(generateExceptionMessage("pipelineStepFinished", exception, pipelineContext),
      region, streamName, partitionKey, accessKey, secretKey)
  }
}
