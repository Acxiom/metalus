package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.steps.util.{DataFrameReaderOptions, DataFrameWriterOptions, StepUtils}
import org.apache.spark.sql.DataFrame

@StepObject
object HiveSteps {

  @StepFunction("3806f23b-478c-4054-b6c1-37f11db58d38",
    "Read a DataFrame from Hive",
    "This step will read a dataFrame in a given format from Hive",
    "Pipeline")
  def readDataFrame(table: String, options: DataFrameReaderOptions, pipelineContext: PipelineContext): DataFrame ={
    StepUtils.buildDataFrameReader(pipelineContext.sparkSession.get, options).table(table)
  }

  @StepFunction("e2b4c011-e71b-46f9-a8be-cf937abc2ec4",
    "Write DataFrame to Hive",
    "This step will write a dataFrame in a given format to Hive",
    "Pipeline")
  def writeDataFrame(dataFrame: DataFrame, table: String, options: DataFrameWriterOptions): Unit = {
    StepUtils.buildDataFrameWriter(dataFrame, options).saveAsTable(table)
  }
}