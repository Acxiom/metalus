package com.acxiom.aws.pipeline.connectors

import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.connectors.{BatchDataConnector, DataConnectorUtilities}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

case class S3DataConnector(override val name: String,
                           override val credentialName: Option[String],
                           override val credential: Option[Credential])
  extends BatchDataConnector with AWSConnector {
  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    val path = source.getOrElse("")
    setSecurity(pipelineContext, path)

    DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions)
      .load(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    val path = destination.getOrElse("")
    setSecurity(pipelineContext, path)
    if (dataFrame.isStreaming) {
      Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, path).start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions)
        .save(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
      None
    }
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
