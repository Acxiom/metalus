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
