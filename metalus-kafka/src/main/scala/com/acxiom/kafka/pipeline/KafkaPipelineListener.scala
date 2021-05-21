package com.acxiom.kafka.pipeline

import com.acxiom.kafka.utils.KafkaUtilities
import com.acxiom.pipeline._

class KafkaPipelineListener(val key: String,
                            val credentialName: String,
                            val credentialProvider: CredentialProvider,
                            topic: String,
                            kafkaNodes: String,
                            clientId: String = "metalus_kafka_pipeline_listener") extends EventBasedPipelineListener {
  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    KafkaUtilities.postMessage(
      generateExecutionMessage("executionStarted", pipelines), topic, kafkaNodes, key, clientId)
    Some(pipelineContext)
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    // TODO Review sending audit information as a separate message? pipelineContext.rootAudit
    KafkaUtilities.postMessage(
      generateExecutionMessage("executionFinished", pipelines), topic, kafkaNodes, key, clientId)
    Some(pipelineContext)
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    KafkaUtilities.postMessage(
      generateExecutionMessage("executionStopped", pipelines), topic, kafkaNodes, key, clientId)
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KafkaUtilities.postMessage(
      generatePipelineMessage("pipelineStarted", pipeline), topic, kafkaNodes, key, clientId)
    Some(pipelineContext)
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KafkaUtilities.postMessage(
      generatePipelineMessage("pipelineFinished", pipeline), topic, kafkaNodes, key, clientId)
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KafkaUtilities.postMessage(
      generatePipelineStepMessage("pipelineStepStarted", pipeline, step, pipelineContext),
      topic, kafkaNodes, key, clientId)
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    KafkaUtilities.postMessage(
      generatePipelineStepMessage("pipelineStepFinished", pipeline, step, pipelineContext),
      topic, kafkaNodes, key, clientId)
    Some(pipelineContext)
  }

  override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    KafkaUtilities.postMessage(
      generateExceptionMessage("registerStepException", exception, pipelineContext),
      topic, kafkaNodes, key, clientId)
  }
}
