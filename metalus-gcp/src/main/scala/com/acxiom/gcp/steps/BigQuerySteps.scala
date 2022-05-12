package com.acxiom.gcp.steps

import com.acxiom.gcp.pipeline.connectors.BigQueryDataConnector
import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame

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
      options.get
    } else {
      DataFrameReaderOptions()
    }
    val connector = BigQueryDataConnector("", "readFromTable", None,  GCPUtilities.convertMapToCredential(credentials))
    connector.load(Some(table), pipelineContext, readerOptions)
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
                   options: Option[DataFrameWriterOptions] = None,
                   pipelineContext: PipelineContext): Unit = {
    // Setup format for BigQuery
    val writerOptions = if (options.isDefined) {
      options.get
    } else {
      DataFrameWriterOptions()
    }
    val connector = BigQueryDataConnector(tempBucket, "writeToTable", None,  GCPUtilities.convertMapToCredential(credentials))
    connector.write(dataFrame, Some(table), pipelineContext, writerOptions)
  }
}
