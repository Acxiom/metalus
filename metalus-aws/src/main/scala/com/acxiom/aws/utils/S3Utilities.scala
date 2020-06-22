package com.acxiom.aws.utils

import com.acxiom.pipeline.PipelineContext

object S3Utilities {
  val MULTIPART_UPLOAD_SIZE = 5242880
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
                         accessKeyId: String,
                         secretAccessKey: String,
                         pipelineContext: PipelineContext): Unit = {
    val protocol = S3Utilities.deriveProtocol(path)
    val sc = pipelineContext.sparkSession.get.sparkContext
    sc.hadoopConfiguration.set(s"fs.$protocol.awsAccessKeyId", accessKeyId)
    sc.hadoopConfiguration.set(s"fs.$protocol.awsSecretAccessKey", secretAccessKey)
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
    s"$protocol:///${prepareS3FilePath(newPath)}"
  }

  /**
    * This function will take the given path and strip any protocol information.
    * @param path A valid path
    * @param bucket An optional bucket name
    * @return A raw path with no protocol information
    */
  def prepareS3FilePath(path: String, bucket: Option[String] = None): String = {
    if (path.startsWith("/")) {
      path.substring(1)
    } else if (bucket.nonEmpty && path.startsWith(s"s3")) {
      path.substring(path.indexOf(s"/${bucket.get}/") +  (2 + bucket.get.length))
    } else {
      path
    }
  }
}
