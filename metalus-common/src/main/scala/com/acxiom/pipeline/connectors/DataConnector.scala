package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait DataConnector extends Connector {
  def load(source: Option[String],
           pipelineContext: PipelineContext,
           readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame
  def write(dataFrame: DataFrame,
            destination: Option[String],
            pipelineContext: PipelineContext,
            writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery]
}

trait BatchDataConnector extends DataConnector {}

trait StreamingDataConnector extends DataConnector {}

trait FileSystemDataConnector extends BatchDataConnector with StreamingDataConnector {

  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    if (readOptions.streaming) {
      val reader = DataConnectorUtilities.buildDataStreamReader(pipelineContext.sparkSession.get, readOptions)
      source.map(reader.load).getOrElse(reader.load())
    } else {
      val reader = DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions)
      source.map(reader.load).getOrElse(reader.load())
    }
  }

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    if (dataFrame.isStreaming) {
      Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, destination.getOrElse("")).start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions).save(destination.getOrElse(""))
      None
    }
  }
}
