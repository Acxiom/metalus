package com.acxiom.metalus.gcp.pipeline

import com.acxiom.metalus._
import com.acxiom.metalus.gcp.utils.GCPUtilities
import com.google.auth.oauth2.GoogleCredentials

class PubSubPipelineListener(val key: String,
                             val credentialName: String,
                             val credentialProvider: CredentialProvider,
                             topicName: String) extends EventBasedPipelineListener {
  private lazy val credential: Option[GoogleCredentials] = GCPUtilities.getCredentialsFromCredentialProvider(credentialProvider, credentialName)

  override def applicationStarted(pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessageInternal(topicName, credential, generateApplicationMessage("applicationStarted"))
    Some(pipelineContext)
  }

  override def applicationComplete(pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessageInternal(topicName, credential, generateApplicationMessage("applicationComplete"))
    pipelineContext.audits.foreach(audit => {
      GCPUtilities.postMessageInternal(topicName, credential, generateAuditMessage("applicationCompleteAudit", audit))
    })

    Some(pipelineContext)
  }

  override def applicationStopped(pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessageInternal(topicName, credential, generateApplicationMessage("applicationStopped"))
    Some(pipelineContext)
  }

  override def pipelineStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val p: Pipeline = getDefaultPipeline(pipelineKey, pipelineContext)
    GCPUtilities.postMessageInternal(topicName, credential, generatePipelineMessage("pipelineStarted", p))
    Some(pipelineContext)
  }

  override def pipelineFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val p: Pipeline = getDefaultPipeline(pipelineKey, pipelineContext)
    GCPUtilities.postMessageInternal(topicName, credential, generatePipelineMessage("pipelineFinished", p))
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val p: Pipeline = getDefaultPipeline(pipelineKey, pipelineContext)
    val step = getDefaultStep(p, pipelineKey)
    GCPUtilities.postMessageInternal(topicName, credential, generatePipelineStepMessage("pipelineStepStarted", p, step, pipelineContext))
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    val p: Pipeline = getDefaultPipeline(pipelineKey, pipelineContext)
    val step = getDefaultStep(p, pipelineKey)
    GCPUtilities.postMessageInternal(topicName, credential, generatePipelineStepMessage("pipelineStepFinished", p, step, pipelineContext))
    Some(pipelineContext)
  }

  override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    GCPUtilities.postMessageInternal(topicName, credential,
      generateExceptionMessage("registerStepException", exception, pipelineContext))
  }

  private def getDefaultPipeline(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext) = {
    val pipeline = getPipeline(pipelineKey, pipelineContext)
    val p = if (pipeline.isDefined) {
      pipeline.get
    } else {
      Pipeline(Some(pipelineKey.pipelineId))
    }
    p
  }

  private def getDefaultStep(pipeline: Pipeline, pipelineStateKey: PipelineStateKey): FlowStep = {
    val step = getStep(pipelineStateKey, pipeline)
    if (step.isDefined) {
      step.get
    } else {
      PipelineStep(pipelineStateKey.stepId)
    }
  }
}
