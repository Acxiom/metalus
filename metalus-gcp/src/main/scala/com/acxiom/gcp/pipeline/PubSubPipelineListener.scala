package com.acxiom.gcp.pipeline

import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline._
import com.google.auth.oauth2.GoogleCredentials

class PubSubPipelineListener(val key: String,
                             val credentialName: String,
                             val credentialProvider: CredentialProvider,
                             topicName: String) extends EventBasedPipelineListener {
  private lazy val credential: Option[GoogleCredentials] = GCPUtilities.getCredentialsFromCredentialProvider(credentialProvider, credentialName)

  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessage(topicName, credential, generateExecutionMessage("executionStarted", pipelines))
    Some(pipelineContext)
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessage(topicName, credential, generateExecutionMessage("executionFinished", pipelines))
    GCPUtilities.postMessage(topicName, credential, generateAuditMessage("executionFinishedAudit", pipelineContext.rootAudit))
    Some(pipelineContext)
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    GCPUtilities.postMessage(topicName, credential, generateExecutionMessage("executionStopped", pipelines))
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessage(topicName, credential, generatePipelineMessage("pipelineStarted", pipeline))
    Some(pipelineContext)
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessage(topicName, credential, generatePipelineMessage("pipelineFinished", pipeline))
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessage(topicName, credential, generatePipelineStepMessage("pipelineStepStarted", pipeline, step, pipelineContext))
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    GCPUtilities.postMessage(topicName, credential, generatePipelineStepMessage("pipelineStepFinished", pipeline, step, pipelineContext))
    Some(pipelineContext)
  }

  override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    GCPUtilities.postMessage(topicName, credential,
      generateExceptionMessage("registerStepException", exception, pipelineContext))
  }
}
