package com.acxiom.metalus.pipeline

import com.acxiom.metalus._
import com.acxiom.metalus.utils.{AWSUtilities, KinesisUtilities}

class KinesisPipelineListener(val key: String,
                              val credentialName: String,
                              val credentialProvider: CredentialProvider,
                              region: String,
                              streamName: String,
                              partitionKey: String) extends EventBasedPipelineListener {
  private lazy val awsCredential = AWSUtilities.getAWSCredential(Some(credentialProvider), credentialName)

  override def applicationStarted(pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generateApplicationMessage("applicationStarted"),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def applicationComplete(pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generateApplicationMessage("applicationComplete"),
      region, streamName, partitionKey, awsCredential)
    pipelineContext.audits.foreach(audit => {
      KinesisUtilities.postMessageWithCredentials(generateAuditMessage("applicationCompleteAudit", audit),
        region, streamName, partitionKey, awsCredential)
    })

    Some(pipelineContext)
  }

  override def applicationStopped(pipelineContext: PipelineContext): Option[PipelineContext] = {
    KinesisUtilities.postMessageWithCredentials(generateApplicationMessage("applicationStopped"),
      region, streamName, partitionKey, awsCredential)
    Some(pipelineContext)
  }

  override def pipelineStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val pipeline = getPipeline(pipelineKey, pipelineContext)
    if (pipeline.isDefined) {
      KinesisUtilities.postMessageWithCredentials(generatePipelineMessage("pipelineStarted", pipeline.get),
        region, streamName, partitionKey, awsCredential)
    }
    Some(pipelineContext)
  }

  override def pipelineFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val pipeline = getPipeline(pipelineKey, pipelineContext)
    if (pipeline.isDefined) {
      KinesisUtilities.postMessageWithCredentials(generatePipelineMessage("pipelineFinished", pipeline.get),
        region, streamName, partitionKey, awsCredential)
    }
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val pipeline = getPipeline(pipelineKey, pipelineContext)
    if (pipeline.isDefined) {
      val step = getStep(pipelineKey, pipeline.get)
      if (step.isDefined) {
        KinesisUtilities.postMessageWithCredentials(generatePipelineStepMessage("pipelineStepStarted", pipeline.get, step.get, pipelineContext),
          region, streamName, partitionKey, awsCredential)
      }
    }
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val pipeline = getPipeline(pipelineKey, pipelineContext)
    if (pipeline.isDefined) {
      val step = getStep(pipelineKey, pipeline.get)
      if (step.isDefined) {
        KinesisUtilities.postMessageWithCredentials(generatePipelineStepMessage("pipelineStepFinished", pipeline.get, step.get, pipelineContext),
          region, streamName, partitionKey, awsCredential)
      }
    }
    Some(pipelineContext)
  }

  override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    KinesisUtilities.postMessageWithCredentials(generateExceptionMessage("pipelineStepFinished", exception, pipelineContext),
      region, streamName, partitionKey, awsCredential)
  }
}
