package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.fs.HDFSFileManager
import org.apache.spark.sql.{DataFrame, Dataset}

@StepObject
object HDFSSteps {

  @StepFunction("87db259d-606e-46eb-b723-82923349640f",
    "Load DataFrame from HDFS path",
    "This step will read a dataFrame from the given HDFS path",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, None, Some("The HDFS path to load data into the DataFrame")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The options to use when loading the DataFrameReader"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def readFromPath(path: String,
                   options: Option[DataFrameReaderOptions] = None,
                   pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext).load(path)
  }

  @StepFunction("8daea683-ecde-44ce-988e-41630d251cb8",
    "Load DataFrame from HDFS paths",
    "This step will read a dataFrame from the given HDFS paths",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("paths" -> StepParameter(None, Some(true), None, None, None, None, Some("The HDFS paths to load data into the DataFrame")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The options to use when loading the DataFrameReader"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def readFromPaths(paths: List[String],
                    options: Option[DataFrameReaderOptions] = None,
                    pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext).load(paths: _*)
  }

  @StepFunction("0a296858-e8b7-43dd-9f55-88d00a7cd8fa",
    "Write DataFrame to HDFS",
    "This step will write a dataFrame in a given format to HDFS",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to write")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The GCS path to write data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options"))))
  def writeToPath(dataFrame: Dataset[_],
                     path: String,
                     options: Option[DataFrameWriterOptions] = None): Unit = {
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions())).save(path)
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
    "InputOutput")
  @StepResults(primaryType = "com.acxiom.pipeline.fs.FileManager",
    secondaryTypes = None)
  def createFileManager(pipelineContext: PipelineContext): Option[HDFSFileManager] = {
    if (pipelineContext.sparkSession.isDefined) {
      Some(HDFSFileManager(pipelineContext.sparkSession.get.sparkContext.getConf))
    } else {
      None
    }
  }
}
