package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame

trait DataConnector {
  def name: String
  def credentialName: Option[String]
  def credential: Option[Credential]
  def load(source: Option[String], pipelineContext: PipelineContext): DataFrame
  def write(dataFrame: DataFrame, destination: Option[String], pipelineContext: PipelineContext): Unit
}

trait BatchDataConnector extends DataConnector {
  def readOptions: DataFrameReaderOptions = DataFrameReaderOptions()
  def writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()
}

trait StreamingDataConnector extends DataConnector {}
