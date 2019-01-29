package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.StepFunction
import org.apache.spark.sql.DataFrame

object HDFSSteps {

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
