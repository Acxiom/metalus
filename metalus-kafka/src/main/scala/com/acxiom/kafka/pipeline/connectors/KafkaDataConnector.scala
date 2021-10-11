package com.acxiom.kafka.pipeline.connectors

import com.acxiom.kafka.utils.KafkaUtilities
import com.acxiom.pipeline.connectors.StreamingDataConnector
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQuery

case class KafkaDataConnector(topics: String,
                              kafkaNodes: String,
                              key: Option[String],
                              keyField: Option[String],
                              override val name: String,
                              override val credentialName: Option[String],
                              override val credential: Option[Credential],
                              clientId: String = "metalus_default_kafka_producer_client",
                              separator: String = ",") extends StreamingDataConnector {
  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    pipelineContext.sparkSession.get
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaNodes)
      .option("subscribe", topics)
      .options(readOptions.options.getOrElse(Map[String, String]()))
      .load()
  }

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    if (dataFrame.isStreaming) {
      Some(dataFrame
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaNodes)
        .option("subscribe", topics)
        .options(writeOptions.options.getOrElse(Map[String, String]()))
        .start())
    } else {
      val col = if (keyField.isDefined && dataFrame.schema.fields.exists(_.name == keyField.get)) {
        dataFrame.col(keyField.get)
      } else if (key.isDefined) {
        lit(key.get)
      } else if (keyField.isDefined) {
        lit(keyField.get)
      } else {
        lit("message_key")
      }
      KafkaUtilities.publishDataFrame(dataFrame, topics, kafkaNodes, col, separator, clientId)
      None
    }
  }
}
