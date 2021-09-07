package com.acxiom.aws.pipeline.connectors

import com.acxiom.aws.utils.{AWSCredential, S3Utilities}
import com.acxiom.pipeline.connectors.{BatchDataConnector, DataConnectorUtilities}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame

case class S3DataConnector(override val name: String,
                           override val credentialName: Option[String],
                           override val credential: Option[Credential],
                           override val readOptions: DataFrameReaderOptions = DataFrameReaderOptions(),
                           override val writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()) extends BatchDataConnector {
  override def load(source: Option[String], pipelineContext: PipelineContext): DataFrame = {
    val path = source.getOrElse("")
    setSecurity(pipelineContext, path)

    DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions)
      .load(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  override def write(dataFrame: DataFrame, destination: Option[String], pipelineContext: PipelineContext): Unit = {
    val path = destination.getOrElse("")
    setSecurity(pipelineContext, path)

    DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions)
      .save(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  private def setSecurity(pipelineContext: PipelineContext, path: String): Unit = {
    val finalCredential = (if (credentialName.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName.get)
    } else {
      credential
    }).asInstanceOf[Option[AWSCredential]]

    if (finalCredential.isDefined) {
      S3Utilities.setS3Authorization(path,
        finalCredential.get.awsAccessKey, finalCredential.get.awsAccessSecret,
        finalCredential.get.awsAccountId, finalCredential.get.awsRole, finalCredential.get.awsPartition, pipelineContext)
    }
  }
}
