package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object InputOutputSteps {
  @StepFunction("a851fad5-ba08-57c9-b5cb-5e2ceb23bbc7",
    "Load File as Data Frame",
    "This step will load a file from the provided URL",
    "Pipeline")
  def loadFile(url: String, format: String, separator: Option[String], pipelineContext: PipelineContext): DataFrame = {
    val dfr = if (separator.isDefined) {
      pipelineContext.sparkSession.get.read.format(format).option("sep", separator.get.toCharArray.head)
    } else {
      pipelineContext.sparkSession.get.read.format(format)
    }

    dfr.load(url)
  }

  @StepFunction("e81e3f51-2d6b-5350-a853-80114f104f19",
    "Write Data Frame to a json file",
    "This step will write a DataFrame from the provided URL",
    "Pipeline")
  def writeJSONFile(dataFrame: DataFrame, url: String, mode: String = "error"): Unit = {
    dataFrame.write.mode(mode).format("json").save(url)
  }
}
