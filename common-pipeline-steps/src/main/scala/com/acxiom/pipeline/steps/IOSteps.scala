package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.steps.util.{DataFrameReaderOptions, DataFrameWriterOptions, StepUtils}
import org.apache.spark.sql.DataFrame

@StepObject
object IOSteps {

  @StepFunction("22fcc0e7-0190-461c-a999-9116b77d5919",
    "Load a DataFrame",
    "This step will read a dataFrame using a DataFrameReaderOptions object",
    "Pipeline")
  def read(dataFrameReaderOptions: DataFrameReaderOptions,
           pipelineContext: PipelineContext): DataFrame ={
    StepUtils.buildDataFrameReader(pipelineContext.sparkSession.get, dataFrameReaderOptions).load()
  }

  @StepFunction("e023fc14-6cb7-44cb-afce-7de01d5cdf00",
    "Write a DataFrame",
    "This step will write a dataFrame using a DataFrameWriterOptions object",
    "Pipeline")
  def write(dataFrame: DataFrame,
                     options: DataFrameWriterOptions): Unit = {
    StepUtils.buildDataFrameWriter(dataFrame, options).save()
  }

}
