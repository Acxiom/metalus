package com.acxiom.metalus.gcp.pipeline.connectors

import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.connectors.{DataRowReader, DataRowWriter, DataStreamOptions, StreamConnector}
import com.acxiom.metalus.gcp.utils.{PubSubDataRowReader, PubSubDataRowWriter, PubSubStreamingDataParser}
import com.acxiom.metalus.utils.DriverUtils
import com.google.pubsub.v1.ReceivedMessage

case class PubSubConnector(projectId: String,
                           override val name: String,
                           override val credentialName: Option[String],
                           override val credential: Option[Credential]) extends StreamConnector with GCPConnector {
  override def getReader(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowReader] = {
    val parameters = pipelineContext.globals.getOrElse(Map())
    val opts = properties.getOrElse(DataStreamOptions(None))
    val format = if (opts.options.contains("pubsubRecordFormat")) {
      opts.options("pubsubRecordFormat").toString
    } else if (parameters.contains("pubsubRecordFormat")) {
      parameters("pubsubRecordFormat")
    } else {
      "csv"
    }
    val separator = if (opts.options.contains("pubsubSeparator")) {
      opts.options("pubsubSeparator").toString
    } else if (parameters.contains("pubsubSeparator")) {
      parameters("pubsubSeparator")
    } else {
      ","
    }
    val defaultParser = PubSubStreamingDataParser(format.toString, separator.toString)
    val parsers = DriverUtils.generateStreamingDataParsers[ReceivedMessage](parameters, Some(List(defaultParser)))
    val credential = getCredential(pipelineContext)
    val subscriptionId = if (opts.options.contains("subscriptionId")) {
      opts.options("subscriptionId").toString
    } else {
      throw DriverUtils.buildPipelineException(Some("A valid subscriptionId must be provided!"), None, Some(pipelineContext))
    }
    Some(PubSubDataRowReader(projectId, subscriptionId, opts.rowBufferSize, parsers, credential))
  }

  override def getWriter(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowWriter] = {
    val parameters = pipelineContext.globals.getOrElse(Map())
    val opts = properties.getOrElse(DataStreamOptions(None))
    val separator = if (opts.options.contains("pubsubSeparator")) {
      opts.options("pubsubSeparator").toString
    } else if (parameters.contains("pubsubSeparator")) {
      parameters("pubsubSeparator")
    } else {
      ","
    }
    val topicId = if (opts.options.contains("topicId")) {
      opts.options("topicId").toString
    } else {
      throw DriverUtils.buildPipelineException(Some("A valid topicId must be provided!"), None, Some(pipelineContext))
    }
    Some(PubSubDataRowWriter(topicId, separator.toString, None, getCredential(pipelineContext)))
  }
}
