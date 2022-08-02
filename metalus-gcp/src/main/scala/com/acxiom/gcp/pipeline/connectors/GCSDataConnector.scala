package com.acxiom.gcp.pipeline.connectors

import com.acxiom.gcp.fs.GCSFileManager
import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline.connectors.{DataConnectorUtilities, FileSystemDataConnector}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

case class GCSDataConnector(override val name: String,
                            override val credentialName: Option[String],
                            override val credential: Option[Credential])
  extends FileSystemDataConnector with GCSConnector {
  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    setSecurity(pipelineContext)
    super.load(source, pipelineContext, readOptions)
  }

  override protected def preparePaths(paths: String): List[String] = super.preparePaths(paths)
    .map(GCSFileManager.prepareGCSFilePath(_))

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    setSecurity(pipelineContext)
    super.write(dataFrame, destination, pipelineContext, writeOptions)
  }

  private def setSecurity(pipelineContext: PipelineContext): Unit = {
    val finalCredential = getCredential(pipelineContext)
    if (finalCredential.isDefined) {
      GCPUtilities.setGCSAuthorization(finalCredential.get.authKey, pipelineContext)
    }
  }
}
