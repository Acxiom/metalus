package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame

case class HDFSDataConnector(override val name: String,
                             override val credentialName: Option[String],
                             override val credential: Option[Credential],
                             override val readOptions: DataFrameReaderOptions = DataFrameReaderOptions(),
                             override val writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()) extends BatchDataConnector {

  override def load(source: Option[String], pipelineContext: PipelineContext): DataFrame =
    DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions).load(source.getOrElse(""))

  override def write(dataFrame: DataFrame, destination: Option[String], pipelineContext: PipelineContext): Unit =
    DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions).save(destination.getOrElse(""))
}
