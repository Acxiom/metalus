package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object HiveSteps {

  @StepFunction("3806f23b-478c-4054-b6c1-37f11db58d38",
    "Read a DataFrame from Hive",
    "This step will read a dataFrame in a given format from Hive",
    "Pipeline",
    "InputOutput")
  def readDataFrame(table: String, options: DataFrameReaderOptions, pipelineContext: PipelineContext): DataFrame ={
    DataFrameSteps.getDataFrameReader(options, pipelineContext).table(table)
  }

  @StepFunction("e2b4c011-e71b-46f9-a8be-cf937abc2ec4",
    "Write DataFrame to Hive",
    "This step will write a dataFrame in a given format to Hive",
    "Pipeline",
    "InputOutput")
  def writeDataFrame(dataFrame: DataFrame, table: String, options: DataFrameWriterOptions): Unit = {
    DataFrameSteps.getDataFrameWriter(dataFrame, options).saveAsTable(table)
  }
}