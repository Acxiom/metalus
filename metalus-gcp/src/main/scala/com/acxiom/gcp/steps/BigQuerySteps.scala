package com.acxiom.gcp.steps

import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameSteps, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame

import java.util.Base64

@StepObject
object BigQuerySteps {
  @StepFunction("3a91eee5-95c1-42dd-9c30-6edc0f9de1ca",
    "Load BigQuery Table",
    "This step will read a DataFrame from a BigQuery table",
    "Pipeline",
    "GCP")
  @StepParameters(Map("table" -> StepParameter(None, Some(true), None, None, None, None, Some("The BigQuery table name to load data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional credentials map"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def readFromTable(table: String,
                   credentials: Option[Map[String, String]],
                   options: Option[DataFrameReaderOptions] = None,
                   pipelineContext: PipelineContext): DataFrame = {
    // Setup format for BigQuery
    val readerOptions = if (options.isDefined) {
      options.get.copy(format = "bigquery")
    } else {
      DataFrameReaderOptions("bigquery")
    }
    // Setup authentication
    val finalOptions = if (credentials.isDefined) {
      readerOptions.copy(options = setBigQueryAuthentication(credentials.get, readerOptions.options))
    } else {
      readerOptions
    }
    DataFrameSteps.getDataFrameReader(finalOptions, pipelineContext).load(table)
  }

  @StepFunction("5b6e114b-51bb-406f-a95a-2a07bc0d05c7",
    "Write BigQuery Table",
    "This step will write a DataFrame to a BigQuery table",
    "Pipeline",
    "GCP")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to write")),
    "table" -> StepParameter(None, Some(true), None, None, None, None, Some("The BigQuery table to write data")),
    "tempBucket" -> StepParameter(None, Some(true), None, None, None, None, Some("The GCS path to write temp data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional credentials map"))))
  def writeToTable(dataFrame: DataFrame,
                  table: String,
                  tempBucket: String,
                  credentials: Option[Map[String, String]],
                  options: Option[DataFrameWriterOptions] = None): Unit = {
    // Setup format for BigQuery
    val writerOptions = if (options.isDefined) {
      options.get.copy(format = "bigquery")
    } else {
      DataFrameWriterOptions("bigquery", options = Some(Map("temporaryGcsBucket" -> tempBucket)))
    }
    // Setup authentication
    val finalOptions = if (credentials.isDefined) {
      val tempOptions = if (writerOptions.options.isDefined) {
        writerOptions.options.get + ("temporaryGcsBucket" -> tempBucket)
      } else {
        Map("temporaryGcsBucket" -> tempBucket)
      }
      writerOptions.copy(options = setBigQueryAuthentication(credentials.get, Some(tempOptions)))
    } else {
      writerOptions
    }
    DataFrameSteps.getDataFrameWriter(dataFrame, finalOptions).save(table)
  }

  private def setBigQueryAuthentication(credentials: Map[String, String],
                                        options: Option[Map[String, String]]): Option[Map[String, String]] = {
    val creds = GCPUtilities.generateCredentialsByteArray(Some(credentials))
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
