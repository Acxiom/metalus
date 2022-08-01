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
                                 override val credential: Option[Credential]) extends BatchDataConnector {
  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    val table = source.getOrElse("")
    val readerOptions = readOptions.copy(format = "com.google.cloud.spark.bigquery.DefaultSource")
    // Setup authentication
    val finalCredential = getCredential(pipelineContext).asInstanceOf[Option[GCPCredential]]
    val finalOptions = if (finalCredential.isDefined) {
      readerOptions.copy(options = setBigQueryAuthentication(finalCredential.get, readerOptions.options, pipelineContext))
    } else {
      readerOptions
    }
    DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, finalOptions).load(table)
  }

  override def write(dataFrame: DataFrame, destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    val table = destination.getOrElse("")
    val options = writeOptions.options.getOrElse(Map[String, String]()) + ("temporaryGcsBucket" -> tempWriteBucket)
    // Setup format for BigQuery
    val writerOptions = writeOptions.copy(format = "com.google.cloud.spark.bigquery.DefaultSource")
    val finalCredential = getCredential(pipelineContext).asInstanceOf[Option[GCPCredential]]
    val finalOptions = if (finalCredential.isDefined) {
      setBigQueryAuthentication(finalCredential.get, Some(options), pipelineContext).get
    } else {
      options
    }
    if (dataFrame.isStreaming) {
      // Use table name in checkpointLocation, but make it path safe
      val streamingOptions = if (!finalOptions.contains("checkpointLocation")) {
        finalOptions + ("checkpointLocation" -> s"$tempWriteBucket/streaming_checkpoints_${table.replaceAll("(?U)[^\\w\\._]+", "_")}")
      } else {
        finalOptions
      } + ("table" -> table)
      Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writerOptions.copy(options = Some(streamingOptions)), "").start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writerOptions.copy(options = Some(finalOptions))).save(table)
      None
    }
  }

  private def setBigQueryAuthentication(credentials: GCPCredential,
                                        options: Option[Map[String, String]],
                                        pipelineContext: PipelineContext): Option[Map[String, String]] = {
    val creds = GCPUtilities.generateCredentialsByteArray(Some(credentials.authKey))
    if (creds.isDefined) {
      GCPUtilities.setGCSAuthorization(credentials.authKey, pipelineContext)
      val encodedCredential = Base64.getEncoder.encodeToString(creds.get)
      if (options.isDefined) {
        Some(options.get + ("credentials" -> encodedCredential,
          "parentProject" -> credentials.authKey.getOrElse("parent_project_id", credentials.authKey("project_id"))))
      } else {
        Some(Map("credentials" -> encodedCredential))
      }
    } else {
      options
    }
  }
}
