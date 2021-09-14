package com.acxiom.gcp.pipeline.connectors

import com.acxiom.gcp.pipeline.GCPCredential
import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline.connectors.{BatchDataConnector, DataConnectorUtilities}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.Base64

case class BigQueryDataConnector(tempWriteBucket: String,
                                 override val name: String,
                                 override val credentialName: Option[String],
                                 override val credential: Option[Credential],
                                 override val readOptions: DataFrameReaderOptions = DataFrameReaderOptions(),
                                 override val writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()) extends BatchDataConnector {
  override def load(source: Option[String], pipelineContext: PipelineContext): DataFrame = {
    val table = source.getOrElse("")
    val readerOptions = readOptions.copy(format = "bigquery")
    // Setup authentication
    val finalCredential = getCredential(pipelineContext).asInstanceOf[Option[GCPCredential]]
    val finalOptions = if (finalCredential.isDefined) {
      readerOptions.copy(options = setBigQueryAuthentication(finalCredential.get, readerOptions.options))
    } else {
      readerOptions
    }
    DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, finalOptions).load(table)
  }

  override def write(dataFrame: DataFrame, destination: Option[String], pipelineContext: PipelineContext): Option[StreamingQuery] = {
    val table = destination.getOrElse("")
    // Setup format for BigQuery
    val writerOptions = writeOptions.copy(format = "bigquery", options = Some(Map("temporaryGcsBucket" -> tempWriteBucket)))
    val finalCredential = getCredential(pipelineContext).asInstanceOf[Option[GCPCredential]]
    val finalOptions = if (finalCredential.isDefined) {
      writerOptions.copy(options = setBigQueryAuthentication(finalCredential.get, writerOptions.options))
    } else {
      writerOptions
    }
    if (dataFrame.isStreaming) {
      Some(dataFrame.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        DataConnectorUtilities.buildDataFrameWriter(batchDF, finalOptions).save(table)
      }.start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, finalOptions).save(table)
      None
    }
  }

  private def setBigQueryAuthentication(credentials: GCPCredential,
                                        options: Option[Map[String, String]]): Option[Map[String, String]] = {
    val creds = GCPUtilities.generateCredentialsByteArray(Some(credentials.authKey))
    if (creds.isDefined) {
      val encodedCredential = Base64.getEncoder.encodeToString(creds.get)
      if (options.isDefined) {
        Some(options.get + ("credentials" -> encodedCredential))
      } else {
        Some(Map("credentials" -> encodedCredential))
      }
    } else {
      options
    }
  }
}
