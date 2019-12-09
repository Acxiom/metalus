package com.acxiom.aws.steps

import com.acxiom.aws.fs.S3FileManager
import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.StepFunction
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameSteps, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame

object S3Steps {
  @StepFunction("bd4a944f-39ad-4b9c-8bf7-6d3c1f356510",
    "Load DataFrame from S3 path",
    "This step will read a DataFrame from the given S3 path",
    "Pipeline",
    "AWS")
  def readFromPath(path: String,
                   accessKeyId: String,
                   secretAccessKey: String,
                   options: DataFrameReaderOptions = DataFrameReaderOptions(),
                   pipelineContext: PipelineContext): DataFrame = {
    S3Utilities.setS3Authorization(path, accessKeyId, secretAccessKey, pipelineContext)
    DataFrameSteps.getDataFrameReader(options, pipelineContext)
      .load(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  @StepFunction("8714aa73-fdb5-4e9f-a8d3-5a813fe14a9e",
    "Load DataFrame from S3 paths",
    "This step will read a dataFrame from the given S3 paths",
    "Pipeline",
    "AWS")
  def readFromPaths(paths: List[String],
                    accessKeyId: String,
                    secretAccessKey: String,
                    options: DataFrameReaderOptions = DataFrameReaderOptions(),
                    pipelineContext: PipelineContext): DataFrame = {
    S3Utilities.setS3Authorization(paths.head, accessKeyId, secretAccessKey, pipelineContext)
    DataFrameSteps.getDataFrameReader(options, pipelineContext)
      .load(paths.map(p => S3Utilities.replaceProtocol(p, S3Utilities.deriveProtocol(p))): _*)
  }

  @StepFunction("7dc79901-795f-4610-973c-f46da63f669c",
    "Write DataFrame to S3",
    "This step will write a DataFrame in a given format to S3",
    "Pipeline",
    "AWS")
  def writeToPath(dataFrame: DataFrame,
                  path: String,
                  accessKeyId: String,
                  secretAccessKey: String,
                  options: DataFrameWriterOptions = DataFrameWriterOptions(),
                  pipelineContext: PipelineContext): Unit = {
    S3Utilities.setS3Authorization(path, accessKeyId, secretAccessKey, pipelineContext)
    DataFrameSteps.getDataFrameWriter(dataFrame, options)
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
  def createFileManager(accessKeyId: String, secretAccessKey: String, region: String, bucket: String): Option[S3FileManager] = {
    Some(new S3FileManager(accessKeyId, secretAccessKey, region, bucket))
  }
}
