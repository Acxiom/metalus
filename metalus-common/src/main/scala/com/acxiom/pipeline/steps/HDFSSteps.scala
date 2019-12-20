package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.fs.HDFSFileManager
import org.apache.spark.sql.DataFrame

@StepObject
object HDFSSteps {

  @StepFunction("87db259d-606e-46eb-b723-82923349640f",
    "Load DataFrame from HDFS path",
    "This step will read a dataFrame from the given HDFS path",
    "Pipeline",
    "InputOutput")
  def readFromPath(path: String,
                   options: DataFrameReaderOptions = DataFrameReaderOptions(),
                   pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options, pipelineContext).load(path)
  }

  @StepFunction("8daea683-ecde-44ce-988e-41630d251cb8",
    "Load DataFrame from HDFS paths",
    "This step will read a dataFrame from the given HDFS paths",
    "Pipeline",
    "InputOutput")
  def readFromPaths(paths: List[String],
                    options: DataFrameReaderOptions = DataFrameReaderOptions(),
                    pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options, pipelineContext).load(paths: _*)
  }

  @StepFunction("0a296858-e8b7-43dd-9f55-88d00a7cd8fa",
    "Write DataFrame to HDFS",
    "This step will write a dataFrame in a given format to HDFS",
    "Pipeline",
    "InputOutput")
  def writeToPath(dataFrame: DataFrame,
                     path: String,
                     options: DataFrameWriterOptions = DataFrameWriterOptions()): Unit = {
    DataFrameSteps.getDataFrameWriter(dataFrame, options).save(path)
  }

  /**
    * Simple function to generate the HDFSFileManager for the local HDFS file system.
    * @param pipelineContext The current pipeline context containing the Spark session
    * @return A FileManager if the spark session is set, otherwise None.
    */
  @StepFunction("e4dad367-a506-5afd-86c0-82c2cf5cd15c",
    "Create HDFS FileManager",
    "Simple function to generate the HDFSFileManager for the local HDFS file system",
    "Pipeline",
    "InputOutput"
  )
  def createFileManager(pipelineContext: PipelineContext): Option[HDFSFileManager] = {
    if (pipelineContext.sparkSession.isDefined) {
      Some(HDFSFileManager(pipelineContext.sparkSession.get.sparkContext.getConf))
    } else {
      None
    }
  }
}
