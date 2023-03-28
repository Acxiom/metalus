package com.acxiom.metalus.aws.pipeline.connectors

import com.acxiom.metalus.aws.utils.{KinesisConsumer, KinesisProducer, KinesisStreamingDataParser}
import com.acxiom.metalus.connectors.{DataRowReader, DataRowWriter, DataStreamOptions, StreamConnector}
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.{Credential, PipelineContext, RetryPolicy}
import software.amazon.awssdk.services.kinesis.model.Record

case class KinesisConnector(streamName: String,
                            region: String,
                            separator: String = ",",
                            partitionKey: Option[String] = None,
                            partitionKeyIndex: Option[Int] = None,
                            retryPolicy: Option[RetryPolicy] = Some(RetryPolicy()),
                            override val name: String,
                            override val credentialName: Option[String],
                            override val credential: Option[Credential]) extends AWSConnector with StreamConnector {
  override def getReader(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowReader] = {
    val parameters = pipelineContext.globals.getOrElse(Map())
    val opts = properties.getOrElse(DataStreamOptions(None))
    val format = if (opts.options.contains("kinesisRecordFormat")) {
      opts.options("kinesisRecordFormat").toString
    } else if (parameters.contains("kinesisRecordFormat")) {
      parameters("kinesisRecordFormat")
    } else {
      "csv"
    }
    val defaultParser = KinesisStreamingDataParser(format.toString, separator)
    val parsers = DriverUtils.generateStreamingDataParsers[Record](parameters, Some(List(defaultParser)))
    val credential = getCredential(pipelineContext)
    val shardId = if (opts.options.contains("shardId")) {
      opts.options("shardId").toString
    } else {
      partitionKey.get
    }
    Some(KinesisConsumer(streamName, region, shardId, opts.rowBufferSize, parsers,
      retryPolicy.getOrElse(RetryPolicy()),
      parameters.getOrElse("streamStartingPosition", "LATEST").toString, credential))
  }

  override def getWriter(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowWriter] =
    Some(KinesisProducer(streamName, region, separator, partitionKey, partitionKeyIndex, retryPolicy, getCredential(pipelineContext)))
}
