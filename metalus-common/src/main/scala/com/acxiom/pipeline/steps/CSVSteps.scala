package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.{DataFrame, Dataset}

@StepObject
object CSVSteps {

  @StepFunction("a2f3e151-cb81-4c69-8475-c1a287bbb4cb",
    "Convert CSV String Dataset to DataFrame",
    "This step will convert the provided CSV string Dataset into a DataFrame that can be passed to other steps",
    "Pipeline",
    "CSV")
  @StepParameters(Map(
    "dataset" -> StepParameter(None, Some(true), None, None, None, None, Some("The dataset containing CSV strings")),
    "dataFrameReaderOptions" -> StepParameter(None, Some(false), None, None, None, None, Some("The CSV parsing options"))))
  def csvDatasetToDataFrame(dataset: Dataset[String],
                             dataFrameReaderOptions: Option[DataFrameReaderOptions] = None,
                             pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(dataFrameReaderOptions.getOrElse(DataFrameReaderOptions("csv")), pipelineContext)
      .csv(dataset)
  }

  @StepFunction("d25209c1-53f6-49ad-a402-257ae756ac2a",
    "Convert CSV String to DataFrame",
    "This step will convert the provided CSV string into a DataFrame that can be passed to other steps",
    "Pipeline",
    "CSV")
  @StepParameters(Map(
    "csvString" -> StepParameter(None, Some(true), None, None, None, None, Some("The csv string to convert to a DataFrame")),
    "delimiter" -> StepParameter(None, Some(false), Some(","), None, None, None, Some("The field delimiter")),
    "recordDelimiter" -> StepParameter(None, Some(false), Some("\n"), None, None, None, Some("The record delimiter")),
    "header" -> StepParameter(None, Some(false), Some("false"), None, None, None, Some("Build header from the first row"))))
  def csvStringToDataFrame(csvString: String,
                           delimiter: Option[String] = None,
                           recordDelimiter: Option[String] = None,
                           header: Option[Boolean] = None,
                           pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get
    import spark.implicits._
    val options = Some(Map("delimiter" -> delimiter.getOrElse(","), "header" -> header.getOrElse("false").toString))
    val ds = csvString.split(recordDelimiter.getOrElse("\n")).toList.toDS()
    csvDatasetToDataFrame(ds, Some(DataFrameReaderOptions("csv", options)), pipelineContext)
  }
}
