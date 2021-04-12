package com.acxiom.aws.utils

import com.acxiom.pipeline.PipelineContext
import org.apache.log4j.Logger

import java.net.URI

object S3Utilities {
  private val logger = Logger.getLogger(getClass)
  val MULTIPART_UPLOAD_SIZE = 52428800
  val MULTIPART_COPY_SIZE = 5368709120L

  /**
    * Given a path, this function will attempt to derive the protocol. If the protocol cannot be determined, then it
    * will default to s3a.
    * @param path A valid path
    * @return The protocol to use for authentication.
    */
  def deriveProtocol(path: String): String = {
    if (path.startsWith("s3")) {
      path.substring(0, path.indexOf(":"))
    } else {
      "s3a"
    }
  }

  /**
    * This function will attempt to set the authorization used when reading or writing a DataFrame.
    *
    * @param path A valid path
    * @param accessKeyId The access key
    * @param secretAccessKey The secret
    * @param pipelineContext The PipelineContext
    */
  def setS3Authorization(path: String,
                         accessKeyId: Option[String],
                         secretAccessKey: Option[String],
                         pipelineContext: PipelineContext): Unit = {
    if (accessKeyId.isDefined && secretAccessKey.isDefined) {
      logger.debug(s"Setting up S3 authorization for $path")
      val protocol = S3Utilities.deriveProtocol(path)
      val sc = pipelineContext.sparkSession.get.sparkContext
      sc.hadoopConfiguration.set(s"fs.$protocol.awsAccessKeyId", accessKeyId.get)
      sc.hadoopConfiguration.set(s"fs.$protocol.access.key", accessKeyId.get)
      sc.hadoopConfiguration.set(s"fs.$protocol.awsSecretAccessKey", secretAccessKey.get)
      sc.hadoopConfiguration.set(s"fs.$protocol.secret.key", secretAccessKey.get)
      sc.hadoopConfiguration.set(s"fs.$protocol.acl.default", "BucketOwnerFullControl")
      sc.hadoopConfiguration.set(s"fs.$protocol.canned.acl", "BucketOwnerFullControl")
    }
  }

  /**
    * This function will attempt to add or replace the protocol in the given path.
    * @param path A calid path
    * @param protocol The protocol to use
    * @return The path with the proper protocol.
    */
  def replaceProtocol(path: String, protocol: String): String = {
    val newPath = if (path.startsWith("s3")) {
      path.substring(path.indexOf(":") + 3)
    } else {
      path
    }
    s"$protocol://${prepareS3FilePath(newPath)}"
  }

  /**
    * This function will take the given path and strip any protocol information.
    * @param path A valid path
    * @return A raw path with no protocol information
    */
  def prepareS3FilePath(path: String, bucket: Option[String] = None): String = {
    val newPath = if (path.startsWith("/")) {
      path.substring(1)
    } else if (path.startsWith(s"s3") && bucket.isDefined) {
      path.substring(path.indexOf(s"${bucket.get}/") + bucket.get.length + 1)
    } else if (path.startsWith(s"s3")) {
      new URI(path).normalize().toString
    } else {
      path
    }
    newPath
  }

  /**
    * Prepares Spark for reading/writing of DataFrames
    * @param pipelineContext The PipelineContext containing the SparkSession
    */
  def configureSparkSession(pipelineContext: PipelineContext): Unit = {
    pipelineContext.sparkSession.get.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    pipelineContext.sparkSession.get.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  }
}
