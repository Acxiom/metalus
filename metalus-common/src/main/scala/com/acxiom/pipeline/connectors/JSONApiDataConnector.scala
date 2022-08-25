package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions, StreamingTriggerOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

case class JSONApiDataConnector(apiHandler: ApiHandler,
                                override val name: String,
                                override val credentialName: Option[String],
                                override val credential: Option[Credential]) extends BatchDataConnector {
  override def load(source: Option[String], pipelineContext: PipelineContext, readOptions: DataFrameReaderOptions): DataFrame =
    apiHandler.toDataFrame(source.getOrElse(""), getCredential(pipelineContext))

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions): Option[StreamingQuery] = {
    val finalCredential = getCredential(pipelineContext)
    if (dataFrame.isStreaming) {
      Some(dataFrame
        .writeStream
        .format(writeOptions.format)
        .options(writeOptions.options.getOrElse(Map[String, String]()))
        .trigger(writeOptions.triggerOptions.getOrElse(StreamingTriggerOptions()).getTrigger)
        .foreach(apiHandler
          .createConnectorWriter(destination.getOrElse(""), finalCredential)
          .asInstanceOf[ForeachWriter[Row]])
        .start())
    } else {
      dataFrame.rdd.foreachPartition(rows => {
        val writer = apiHandler.createConnectorWriter(destination.getOrElse(""), finalCredential)
        writer.open()
        rows.foreach(writer.process)
        writer.close()
      })
      None
    }
  }
}
