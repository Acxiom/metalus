package com.acxiom.aws.pipeline.connectors

import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.connectors.FileSystemDataConnector
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

case class S3DataConnector(override val name: String,
                           override val credentialName: Option[String],
                           override val credential: Option[Credential])
  extends FileSystemDataConnector with AWSConnector {
  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    val path = source.getOrElse("")
    setSecurity(pipelineContext, path)
    super.load(source, pipelineContext, readOptions)
  }

  override protected def preparePaths(paths: String): List[String] = super.preparePaths(paths)
    .map(p => S3Utilities.replaceProtocol(p, S3Utilities.deriveProtocol(p)))

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    setSecurity(pipelineContext, destination.getOrElse(""))
    super.write(dataFrame, destination, pipelineContext, writeOptions)
  }

  private def setSecurity(pipelineContext: PipelineContext, path: String): Unit = {
    val finalCredential = getCredential(pipelineContext)

    if (finalCredential.isDefined) {
      S3Utilities.setS3Authorization(path,
        finalCredential.get.awsAccessKey, finalCredential.get.awsAccessSecret,
        finalCredential.get.awsAccountId, finalCredential.get.awsRole, finalCredential.get.awsPartition,
        finalCredential.get.duration, pipelineContext)
    }
  }
}
