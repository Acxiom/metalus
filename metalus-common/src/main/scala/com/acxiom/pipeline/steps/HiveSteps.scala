package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.DataFrame

@StepObject
object HiveSteps {

  @StepFunction("3806f23b-478c-4054-b6c1-37f11db58d38",
    "Read a DataFrame from Hive",
    "This step will read a dataFrame in a given format from Hive",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("table" -> StepParameter(None, Some(true), description = Some("The name of the table to read")),
    "options" -> StepParameter(None, Some(false), description = Some("The DataFrameReaderOptions to use"))))
  def readDataFrame(table: String, options: Option[DataFrameReaderOptions] = None, pipelineContext: PipelineContext): DataFrame ={
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext).table(table)
  }

  @StepFunction("e2b4c011-e71b-46f9-a8be-cf937abc2ec4",
    "Write DataFrame to Hive",
    "This step will write a dataFrame in a given format to Hive",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to write")),
    "table" -> StepParameter(None, Some(true), description = Some("The name of the table to write to")),
    "options" -> StepParameter(None, Some(false), description = Some("The DataFrameWriterOptions to use"))))
  def writeDataFrame(dataFrame: DataFrame, table: String, options: Option[DataFrameWriterOptions] = None): Unit = {
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions())).saveAsTable(table)
  }
}
