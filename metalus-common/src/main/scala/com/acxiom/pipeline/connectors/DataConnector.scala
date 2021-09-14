package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait DataConnector {
  def name: String
  def credentialName: Option[String]
  def credential: Option[Credential]
  def readOptions: DataFrameReaderOptions = DataFrameReaderOptions()
  def writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()
  def load(source: Option[String], pipelineContext: PipelineContext): DataFrame
  def write(dataFrame: DataFrame, destination: Option[String], pipelineContext: PipelineContext): Option[StreamingQuery]

  protected def getCredential(pipelineContext: PipelineContext): Option[Credential] = {
    if (credentialName.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName.get)
    } else {
      credential
    }
  }
}

trait BatchDataConnector extends DataConnector {}

trait StreamingDataConnector extends DataConnector {}
