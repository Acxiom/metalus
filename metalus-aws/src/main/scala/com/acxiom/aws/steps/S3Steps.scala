package com.acxiom.aws.steps

import com.acxiom.aws.fs.S3FileManager
import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameSteps, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame

@StepObject
object S3Steps {
  @StepFunction("bd4a944f-39ad-4b9c-8bf7-6d3c1f356510",
    "Load DataFrame from S3 path",
    "This step will read a DataFrame from the given S3 path",
    "Pipeline",
    "AWS")
  def readFromPath(path: String,
                   accessKeyId: Option[String] = None,
                   secretAccessKey: Option[String] = None,
                   options: Option[DataFrameReaderOptions] = None,
                   pipelineContext: PipelineContext): DataFrame = {
    if (accessKeyId.isDefined && secretAccessKey.isDefined) {
      S3Utilities.setS3Authorization(path, accessKeyId.get, secretAccessKey.get, pipelineContext)
    }
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext)
      .load(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  @StepFunction("8714aa73-fdb5-4e9f-a8d3-5a813fe14a9e",
    "Load DataFrame from S3 paths",
    "This step will read a dataFrame from the given S3 paths",
    "Pipeline",
    "AWS")
  def readFromPaths(paths: List[String],
                    accessKeyId: Option[String] = None,
                    secretAccessKey: Option[String] = None,
                    options: Option[DataFrameReaderOptions] = None,
                    pipelineContext: PipelineContext): DataFrame = {
    if (accessKeyId.isDefined && secretAccessKey.isDefined) {
      S3Utilities.setS3Authorization(paths.head, accessKeyId.get, secretAccessKey.get, pipelineContext)
    }
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext)
      .load(paths.map(p => S3Utilities.replaceProtocol(p, S3Utilities.deriveProtocol(p))): _*)
  }

  @StepFunction("7dc79901-795f-4610-973c-f46da63f669c",
    "Write DataFrame to S3",
    "This step will write a DataFrame in a given format to S3",
    "Pipeline",
    "AWS")
  def writeToPath(dataFrame: DataFrame,
                  path: String,
                  accessKeyId: Option[String] = None,
                  secretAccessKey: Option[String] = None,
                  options: Option[DataFrameWriterOptions] = None,
                  pipelineContext: PipelineContext): Unit = {
    if (accessKeyId.isDefined && secretAccessKey.isDefined) {
      S3Utilities.setS3Authorization(path, accessKeyId.get, secretAccessKey.get, pipelineContext)
    }
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions()))
      .save(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  /**
   * Simple function to generate the HDFSFileManager for the local S3 file system.
   *
   * @param accessKeyId     The AWS access key to use when interacting with the S3 bucket
   * @param secretAccessKey The AWS secret to use when interactin with the S3 bucket
   * @param region          The AWS region this bucket should be accessed in
   * @param bucket          The bucket to use for this file system.
   * @return A FileManager that can interact with the specified S3 bucket.
   */
  @StepFunction("cc4694b9-5e54-4b12-8088-ed4ced056efd",
    "Create S3 FileManager",
    "Simple function to generate the S3FileManager for a S3 file system",
    "Pipeline",
    "AWS"
  )
  def createFileManager(region: String,
                        bucket: String,
                        accessKeyId: Option[String] = None,
                        secretAccessKey: Option[String] = None): Option[S3FileManager] = {
    Some(new S3FileManager(region, bucket, accessKeyId, secretAccessKey))
  }
}
