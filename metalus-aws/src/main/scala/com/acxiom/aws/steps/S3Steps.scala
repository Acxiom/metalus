package com.acxiom.aws.steps

import com.acxiom.aws.fs.S3FileManager
import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameSteps, DataFrameWriterOptions}
import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.sql.DataFrame

@StepObject
object S3Steps {
  @StepFunction("75dca4ff-d4c1-4171-8cea-a303c17d461d",
    "Register S3 FS Providers",
    "Registers the S3N and S3A File System providers",
    "Pipeline",
    "AWS")
  def registerS3FileSystems(pipelineContext: PipelineContext): Unit = {
    S3Utilities.registerS3FileSystems(pipelineContext)
  }

  @StepFunction("18290ec4-93e1-427c-8f46-2eb48dd7d1fd",
    "Setup S3 Authentication",
    "This step will setup authentication read for DataFrames using the provided key and secret",
    "Pipeline",
    "AWS")
  @StepParameters(Map("accessKeyId" -> StepParameter(None, Some(true), None, None, None, None, Some("The API key to use for S3 access")),
    "secretAccessKey" -> StepParameter(None, Some(true), None, None, None, None, Some("The API secret to use for S3 access"))))
  def setupS3Authentication(accessKeyId: String,
                            secretAccessKey: String,
                            pipelineContext: PipelineContext): Unit = {
    val sc = pipelineContext.sparkSession.get.sparkContext
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
    sc.hadoopConfiguration.set(s"fs.s3a.access.key", accessKeyId)
    sc.hadoopConfiguration.set(s"fs.s3a.secret.key", secretAccessKey)
    sc.hadoopConfiguration.set(s"fs.s3a.acl.default", "BucketOwnerFullControl")
    sc.hadoopConfiguration.set(s"fs.s3a.canned.acl", "BucketOwnerFullControl")
  }

  @StepFunction("bd4a944f-39ad-4b9c-8bf7-6d3c1f356510",
    "Load DataFrame from S3 path",
    "This step will read a DataFrame from the given S3 path",
    "Pipeline",
    "AWS")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, None, Some("The S3 path to load data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "accessKeyId" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API key to use for S3 access")),
    "secretAccessKey" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API secret to use for S3 access"))))
  def readFromPath(path: String,
                   accessKeyId: Option[String] = None,
                   secretAccessKey: Option[String] = None,
                   options: Option[DataFrameReaderOptions] = None,
                   pipelineContext: PipelineContext): DataFrame = {
    S3Utilities.setS3Authorization(path, accessKeyId, secretAccessKey, pipelineContext)
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext)
      .load(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  @StepFunction("8714aa73-fdb5-4e9f-a8d3-5a813fe14a9e",
    "Load DataFrame from S3 paths",
    "This step will read a dataFrame from the given S3 paths",
    "Pipeline",
    "AWS")
  @StepParameters(Map("paths" -> StepParameter(None, Some(true), None, None, None, None, Some("The S3 paths to load data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "accessKeyId" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API key to use for S3 access")),
    "secretAccessKey" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API secret to use for S3 access"))))
  def readFromPaths(paths: List[String],
                    accessKeyId: Option[String] = None,
                    secretAccessKey: Option[String] = None,
                    options: Option[DataFrameReaderOptions] = None,
                    pipelineContext: PipelineContext): DataFrame = {
    S3Utilities.setS3Authorization(paths.head, accessKeyId, secretAccessKey, pipelineContext)
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext)
      .load(paths.map(p => S3Utilities.replaceProtocol(p, S3Utilities.deriveProtocol(p))): _*)
  }

  @StepFunction("7dc79901-795f-4610-973c-f46da63f669c",
    "Write DataFrame to S3",
    "This step will write a DataFrame in a given format to S3",
    "Pipeline",
    "AWS")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Kinesis stream")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The S3 path to write data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "accessKeyId" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API key to use for S3 access")),
    "secretAccessKey" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API secret to use for S3 access"))))
  def writeToPath(dataFrame: DataFrame,
                  path: String,
                  accessKeyId: Option[String] = None,
                  secretAccessKey: Option[String] = None,
                  options: Option[DataFrameWriterOptions] = None,
                  pipelineContext: PipelineContext): Unit = {
    S3Utilities.setS3Authorization(path, accessKeyId, secretAccessKey, pipelineContext)
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions()))
      .save(S3Utilities.replaceProtocol(path, S3Utilities.deriveProtocol(path)))
  }

  /**
    * Simple function to generate the S3FileManager for the local S3 file system.
    *
    * @param accessKeyId     The AWS access key to use when interacting with the S3 bucket
    * @param secretAccessKey The AWS secret to use when interaction with the S3 bucket
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
  @StepParameters(Map("bucket" -> StepParameter(None, Some(true), None, None, None, None, Some("The S3 bucket")),
    "region" -> StepParameter(None, Some(true), None, None, None, None, Some("The region of the S3 bucket")),
    "accessKeyId" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API key to use for S3 access")),
    "secretAccessKey" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional API secret to use for S3 access"))))
  def createFileManager(region: String,
                        bucket: String,
                        accessKeyId: Option[String] = None,
                        secretAccessKey: Option[String] = None): Option[S3FileManager] = {
    Some(new S3FileManager(region, bucket, accessKeyId, secretAccessKey))
  }

  /**
    * Simple function to generate the S3FileManager for the local S3 file system.
    *
    * @param s3Client The existing AWS S3 client
    * @param bucket   The bucket to use for this file system.
    * @return A FileManager that can interact with the specified S3 bucket.
    */
  @StepFunction("0e3bcadd-2d14-408f-982f-32ffd879d795d",
    "Create S3 FileManager with Client",
    "Simple function to generate the S3FileManager for a S3 file system using an existing client",
    "Pipeline",
    "AWS"
  )
  @StepParameters(Map("bucket" -> StepParameter(None, Some(true), None, None, None, None, Some("The S3 bucket")),
    "s3Client" -> StepParameter(None, Some(true), None, None, None, None, Some("An existing S3 client use to access the bucket"))))
  def createFileManagerWithClient(s3Client: AmazonS3, bucket: String): Option[S3FileManager] = {
    Some(new S3FileManager(s3Client, bucket))
  }
}
