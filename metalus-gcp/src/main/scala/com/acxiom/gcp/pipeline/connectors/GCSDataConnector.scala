package com.acxiom.gcp.pipeline.connectors

import com.acxiom.gcp.fs.GCSFileManager
import com.acxiom.gcp.pipeline.GCPCredential
import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline.connectors.{BatchDataConnector, DataConnectorUtilities}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame

case class GCSDataConnector(override val name: String,
                            override val credentialName: Option[String],
                            override val credential: Option[Credential],
                            override val readOptions: DataFrameReaderOptions = DataFrameReaderOptions(),
                            override val writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()) extends BatchDataConnector {
  override def load(source: Option[String], pipelineContext: PipelineContext): DataFrame = {
    val path = source.getOrElse("")
    setSecurity(pipelineContext)
    DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions)
      .load(GCSFileManager.prepareGCSFilePath(path))
  }

  override def write(dataFrame: DataFrame, destination: Option[String], pipelineContext: PipelineContext): Unit = {
    val path = destination.getOrElse("")
    setSecurity(pipelineContext)
    DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions)
      .save(GCSFileManager.prepareGCSFilePath(path))
  }

  private def setSecurity(pipelineContext: PipelineContext): Unit = {
    val finalCredential = (if (credentialName.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName.get)
    } else {
      credential
    }).asInstanceOf[Option[GCPCredential]]
    if (finalCredential.isDefined) {
      GCPUtilities.setGCSAuthorization(finalCredential.get.authKey, pipelineContext)
    }
  }
}
