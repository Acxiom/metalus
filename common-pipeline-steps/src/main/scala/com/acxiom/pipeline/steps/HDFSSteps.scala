package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object HDFSSteps {

  @StepFunction("87db259d-606e-46eb-b723-82923349640f",
    "Write DataFrame to HDFS",
    "This step will write a dataFrame in a given format to HDFS",
    "Pipeline")
  def readFromHDFS(path: String,
                   format: String = "parquet",
                   properties: Option[Map[String, String]] = None,
                   pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get

    val reader = spark.read.format(format)
    if(properties.isDefined){
      reader.options(properties.get).load(path)
    } else {
      reader.load(path)
    }
  }


  @StepFunction("0a296858-e8b7-43dd-9f55-88d00a7cd8fa",
    "Write DataFrame to HDFS",
    "This step will write a dataFrame in a given format to HDFS",
    "Pipeline")
  def writeDataFrame(dataFrame: DataFrame,
                     path: String,
                     format: String = "parquet",
                     saveMode: String = "Overwrite"): Unit = {
    dataFrame.write.format(format)
      .mode(saveMode)
      .save(path)
  }
}
