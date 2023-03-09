package com.acxiom.metalus.fs

import com.acxiom.metalus.utils.DriverUtils.buildPipelineException
import com.acxiom.metalus.utils.{AWSUtilities, DefaultAWSCredential, S3Utilities}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{S3Client, S3ClientBuilder}
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.CopyRequest

import java.io.{FileNotFoundException, InputStream, OutputStream}
import java.util
import scala.jdk.CollectionConverters._

class S3FileManager(s3Client: S3Client, bucket: String) extends FileManager {
  private val logger = LoggerFactory.getLogger(getClass)

  def this(region: String,
           bucket: String,
           accessKeyId: Option[String] = None,
           secretAccessKey: Option[String] = None,
           accountId: Option[String] = None,
           role: Option[String] = None,
           partition: Option[String] = None) = {
    this({
      val credential = if (accessKeyId.isDefined || role.isDefined) {
        Some(new DefaultAWSCredential(Map[String, Any]("credentialName" -> "fileManagerCredentials",
          "accountId" -> accountId,
          "role" -> role,
          "accessKeyId" -> accessKeyId,
          "secretAccessKey" -> secretAccessKey,
          "partition" -> partition)))
      } else {
        None
      }
      AWSUtilities.setupCredentialProvider(S3Client.builder(), credential).asInstanceOf[S3ClientBuilder]
    }.region(Region.of(region)).build(), bucket)
  }

  /**
   * Connect to the file system
   */
  override def connect(): Unit = {
    // Does nothing
  }

  /**
   * Checks the path to determine whether it exists or not.
   *
   * @param path The path to verify
   * @return True if the path exists, otherwise false.
   */
  override def exists(path: String): Boolean = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    try {
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(finalPath).build())
      true
    } catch {
      case _: NoSuchKeyException => directoryExists(finalPath)
      case t: Throwable =>
        logger.warn(s"Unknown exception checking key existence: ${t.getMessage}", t)
        false
    }
  }

  /**
   * Returns a list of file names at the given path.
   *
   * @param path      The path to list.
   * @param recursive Flag indicating whether to run a recursive or simple listing.
   * @return A list of files at the given path
   */
  override def getFileListing(path: String, recursive: Boolean = true): List[FileResource] = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    try {
      val objListing = if (recursive) {
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(finalPath).build())
      } else {
        val req = ListObjectsV2Request.builder().bucket(bucket).delimiter("/")
          .prefix(if (!finalPath.endsWith("/")) finalPath + "/" else finalPath).build()
        s3Client.listObjectsV2(req)
      }
      objListing.contents().asScala.map(content => S3FileResource(s3Client, bucket, content)).toList
    } catch {
      case r: NoSuchBucketException =>
        logger.warn(s"Bucket $bucket does not exist")
        throw buildPipelineException(Some(s"Bucket $bucket does not exist"), Some(r), None)
      case t: Throwable =>
        logger.warn(s"Unknown exception checking key existence: ${t.getMessage}", t)
        throw buildPipelineException(Some(s"Unknown exception while listing $finalPath"), Some(t), None)
    }
  }

  /**
   * Disconnect from the file system
   */
  override def disconnect(): Unit = {
    // Does nothing
  }

  private def directoryExists(path: String): Boolean = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    try {
      val req = ListObjectsV2Request.builder()
        .bucket(bucket)
        .delimiter("/")
        .prefix(if (!finalPath.endsWith("/")) finalPath + "/" else finalPath)
        .maxKeys(1)
        .build()
      s3Client.listObjectsV2(req).hasContents
    } catch {
      case r: NoSuchBucketException =>
        logger.warn(s"Bucket $bucket does not exist")
        throw buildPipelineException(Some(s"Bucket $bucket does not exist"), Some(r), None)
      case t: Throwable => false
    }
  }

  override def getFileResource(path: String): FileResource = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    try {
      val list = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(finalPath).build()).contents()
      if (list.size() == 1) {
        S3FileResource(s3Client, bucket, list.get(0))
      } else {
        val summary = S3Object.builder()
          .key(finalPath)
          .size(-1)
          .build()
        S3FileResource(s3Client, bucket, summary, isDirectory = true)
      }
    } catch {
      case r: NoSuchBucketException =>
        logger.warn(s"Bucket $bucket does not exist")
        throw buildPipelineException(Some(s"Bucket $bucket does not exist"), Some(r), None)
      case t: Throwable =>
        logger.warn(s"Unknown exception when attempting to locate: $finalPath")
        throw buildPipelineException(Some(s"Unknown exception when attempting to locate: $finalPath"), Some(t), None)
    }
  }
}

case class S3FileResource(s3Client: S3Client, bucket: String, summary: S3Object, isDirectory: Boolean = false) extends FileResource {

  override def fileName: String = summary.key().split('/').last

  override def fullName: String = summary.key()

  override def directory: Boolean = isDirectory

  override def rename(destPath: String): Boolean = {
    val source = S3Utilities.prepareS3FilePath(fileName, Some(bucket))
    val destination = S3Utilities.prepareS3FilePath(destPath, Some(bucket))
    val copyObjectRequest = CopyObjectRequest.builder.sourceBucket(bucket).sourceKey(source).destinationBucket(bucket).destinationKey(destination).build()
    val fileLength = summary.size()
    try {
      if (fileLength < S3Utilities.MULTIPART_COPY_SIZE) {
        s3Client.copyObject(copyObjectRequest)
      } else {
        // TODO [2.0 Review] See about using async client
        val transferManager = S3TransferManager.builder().build()
        val copyRequest = CopyRequest.builder.copyObjectRequest(copyObjectRequest).build

        val copy = transferManager.copy(copyRequest)
        copy.completionFuture().join()
      }

      s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(source).build())
      true
    } catch {
      case r: ObjectNotInActiveTierErrorException =>
        val message = s"File only exists in Glacier storage tier and cannot be copied: $source"
        logger.warn(message)
        throw buildPipelineException(Some(message), Some(r), None)
      case t: Throwable =>
        logger.warn(s"Unable to rename file from $source to $destination", t)
        false
    }
  }

  override def delete: Boolean = {
    // get a list of all objects with this prefix (in case filePath is a folder)
    val finalPath = S3Utilities.prepareS3FilePath(fileName, Some(bucket))
    try {
      val s3ObjectList = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(finalPath).build()).contents()
      s3ObjectList.asScala.toArray
        .foreach(f => s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(f.key).build()))
      true
    } catch {
      case r: NoSuchBucketException =>
        logger.warn(s"Bucket $bucket does not exist")
        throw buildPipelineException(Some(s"Bucket $bucket does not exist"), Some(r), None)
      case t: Throwable =>
        logger.warn(s"Unknown exception when attempting to delete: $finalPath", t)
        false
    }
  }

  override def size: Long = if (summary.size != -1) {
    summary.size
  } else {
    throw new FileNotFoundException(s"File not found when attempting to get size,inputPath=$fullName")
  }

  override def getInputStream(bufferSize: Int): InputStream = {
    val finalPath = S3Utilities.prepareS3FilePath(fileName, Some(bucket))
    try {
      s3Client.getObject(GetObjectRequest.builder()
        .bucket(bucket)
        .key(finalPath).build())
    } catch {
      case r: NoSuchKeyException =>
        val message = s"Resource $finalPath does not exist"
        logger.warn(message, r)
        throw buildPipelineException(Some(message), Some(r), None)
      case r: InvalidObjectStateException =>
        val message = s"Resource $finalPath is archived and inaccessible until restored"
        logger.warn(message, r)
        throw buildPipelineException(Some(message), Some(r), None)
      case t: Throwable =>
        val message = s"Unknown exception when accessing $finalPath"
        logger.warn(message, t)
        throw buildPipelineException(Some(message), Some(t), None)
    }
  }

  override def getOutputStream(append: Boolean, bufferSize: Int): OutputStream = {
    val finalPath = S3Utilities.prepareS3FilePath(fullName, Some(bucket))
    try {
      new S3OutputStream(s3Client, bucket, finalPath, Some(bufferSize))
    } catch {
      case e: Throwable =>
        val message = s"Unable to create OutputStream for $finalPath"
        logger.warn(message, e)
        throw buildPipelineException(Some(message), Some(e), None)
    }
  }

  override def copy(destination: FileResource): Boolean = {
    destination match {
      case resource: S3FileResource =>
        val copyReq = CopyObjectRequest.builder
          .sourceBucket(bucket)
          .sourceKey(fullName)
          .destinationBucket(resource.bucket)
          .destinationKey(resource.fullName)
          .build()
        try {
          s3Client.copyObject(copyReq)
          true
        } catch {
          case r: ObjectNotInActiveTierErrorException =>
            val message = s"File only exists in Glacier storage tier and cannot be copied: $fullName"
            logger.warn(message)
            throw buildPipelineException(Some(message), Some(r), None)
          case e: Throwable =>
            logger.warn(s"Unable to use native copy: ${e.getMessage}")
            super.copy(destination)
        }
      case _ => super.copy(destination)
    }
  }
}
