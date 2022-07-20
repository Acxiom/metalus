package com.acxiom.aws.fs

import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.fs.{FileInfo, FileManager}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.{FileNotFoundException, InputStream, OutputStream}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

class S3FileManager(s3Client: AmazonS3, bucket: String) extends FileManager {

  def this(region: String,
           bucket: String,
           accessKeyId: Option[String] = None,
           secretAccessKey: Option[String] = None,
           accountId: Option[String] = None,
           role: Option[String] = None,
           partition: Option[String] = None) = {
    this({
      val builder = AmazonS3ClientBuilder.standard()
      if (accountId.isDefined && accountId.get.trim.nonEmpty && role.isDefined && role.get.trim.nonEmpty) {
        val sessionCredentials = S3Utilities.assumeRole(accountId.get, role.get, partition).getCredentials
        builder.withCredentials(new AWSStaticCredentialsProvider(new BasicSessionCredentials(sessionCredentials.getAccessKeyId,
          sessionCredentials.getSecretAccessKey,
          sessionCredentials.getSessionToken)))
      } else if (accessKeyId.isDefined && secretAccessKey.isDefined) {
        builder
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId.get, secretAccessKey.get)))
      } else {
        builder
      }
      }.withRegion(region).build(), bucket)
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
    s3Client.doesObjectExist(bucket, finalPath) || directoryExists(finalPath)
  }

  /**
    * Creates a buffered input stream for the provided path.
    *
    * @param path       The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @return A buffered input stream
    */
  override def getInputStream(path: String, bufferSize: Int): InputStream = {
    val file = s3Client.getObject(bucket, S3Utilities.prepareS3FilePath(path, Some(bucket)))
    file.getObjectContent
  }

  /**
    * Creates a buffered output stream for the provided path.
    *
    * @param path       The path where data will be written.
    * @param append     Boolean flag indicating whether data should be appended. Default is true
    * @param bufferSize The buffer size to apply to the stream
    * @return
    */
  override def getOutputStream(path: String, append: Boolean, bufferSize: Int): OutputStream = {
    new S3OutputStream(s3Client, bucket, S3Utilities.prepareS3FilePath(path, Some(bucket)))
  }

  /**
    * Will attempt to rename the provided path to the destination path.
    *
    * @param path     The path to rename.
    * @param destPath The destination path.
    * @return True if the path could be renamed.
    */
  override def rename(path: String, destPath: String): Boolean = {
    val source = S3Utilities.prepareS3FilePath(path, Some(bucket))
    val destination = S3Utilities.prepareS3FilePath(destPath, Some(bucket))

    val fileLength = s3Client.getObjectMetadata(bucket, source).getContentLength
    if (fileLength < S3Utilities.MULTIPART_COPY_SIZE) {
      s3Client.copyObject(new CopyObjectRequest(bucket, source, bucket, destination))
    } else {
      val request = s3Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, destination))
      val etags = copyS3Part(S3CopyInfo(s3Client, bucket, source, bucket, destination,
        request.getUploadId, fileLength), 0L, 1, List[PartETag]())

      s3Client.completeMultipartUpload(
        new CompleteMultipartUploadRequest(bucket, destination, request.getUploadId, etags.asJava))
    }

    s3Client.deleteObject(bucket, source)
    true
  }

  /**
    * Attempts to delete the provided path.
    *
    * @param path The path to delete.
    * @return True if the path could be deleted.
    */
  override def deleteFile(path: String): Boolean = {
    // get a list of all objects with this prefix (in case filePath is a folder)
    val s3ObjectList = s3Client.listObjects(bucket, S3Utilities.prepareS3FilePath(path, Some(bucket))).getObjectSummaries
    s3ObjectList.toArray.foreach(f => {
      val fileKey = f.asInstanceOf[S3ObjectSummary].getKey
      s3Client.deleteObject(new DeleteObjectRequest(bucket, fileKey))
    })
    true
  }

  /**
    * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
    *
    * @param path The path to the file
    * @return size of the given file
    */
  override def getSize(path: String): Long = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    if (s3Client.doesObjectExist(bucket, finalPath)) {
      // get a list of all objects with this prefix (in case path is a folder)
      val s3ObjectList = s3Client.listObjects(bucket, finalPath).getObjectSummaries

      // convert to Scala list and sum the size of all objects with the prefix
      s3ObjectList.asScala.map(_.getSize).sum
    } else {
      throw new FileNotFoundException(s"File not found when attempting to get size,inputPath=$path")
    }
  }

  /**
    * Returns a list of file names at the given path.
    *
    * @param path The path to list.
    * @param recursive Flag indicating whether to run a recursive or simple listing.
    * @return A list of files at the given path
    */
  override def getFileListing(path: String, recursive: Boolean = true): List[FileInfo] = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    val objListing = if (recursive) {
      s3Client.listObjects(bucket, finalPath)
    } else {
      val req = new ListObjectsRequest()
        .withBucketName(bucket)
        .withDelimiter("/")
        .withPrefix(if (!finalPath.endsWith("/")) finalPath + "/" else finalPath)
      s3Client.listObjects(req)
    }
    nextObjectBatch(s3Client, objListing).map(_.copy(path = Some(s"s3://$bucket")))
  }

  /**
   * Returns a FileInfo objects for the given path
   * @param path The path to get a status of.
   * @return A FileInfo object for the path given.
   */
  override def getStatus(path: String): FileInfo = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    if (s3Client.doesObjectExist(bucket, finalPath)) {
      val fs = s3Client.getObjectMetadata(bucket, finalPath)
      FileInfo(finalPath, fs.getContentLength, directory = false, Some(s"s3://$bucket"))
    } else if (directoryExists(finalPath)){
      FileInfo(finalPath, 0, directory = true, Some(s"s3://$bucket"))
    } else {
      throw new FileNotFoundException(s"File not found when attempting to get size,inputPath=$path")
    }
  }

  /**
    * Disconnect from the file system
    */
  override def disconnect(): Unit = {
    // Does nothing
  }

  @tailrec
  private def nextObjectBatch(s3Client: AmazonS3, listing: ObjectListing, keys: List[FileInfo] = Nil): List[FileInfo] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(summary => FileInfo(summary.getKey, summary.getSize, directory = false)).toList

    if (listing.isTruncated) {
      nextObjectBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: keys)
    }
    else {
      pageKeys ::: keys
    }
  }

  @tailrec
  private def copyS3Part(copyInfo: S3CopyInfo, bytePosition: Long, partNum: Int, etags: List[PartETag]): List[PartETag] = {
    val lastByte = Math.min(bytePosition + S3Utilities.MULTIPART_COPY_SIZE - 1, copyInfo.fileLength - 1)
    val part = copyInfo.s3Client.copyPart(new CopyPartRequest()
      .withSourceBucketName(copyInfo.sourceBucket)
      .withSourceKey(copyInfo.sourceKey)
      .withDestinationBucketName(copyInfo.destinationBucket)
      .withDestinationKey(copyInfo.destinationKey)
      .withUploadId(copyInfo.uploadId)
      .withFirstByte(bytePosition)
      .withLastByte(lastByte)
      .withPartNumber(partNum))
    val updatePosition = bytePosition + S3Utilities.MULTIPART_COPY_SIZE
    val updatedPartNum = partNum + 1
    if (updatePosition < copyInfo.fileLength) {
      copyS3Part(copyInfo, updatePosition, updatedPartNum, etags :+ new PartETag(part.getPartNumber, part.getETag))
    } else {
      etags :+ new PartETag(part.getPartNumber, part.getETag)
    }
  }

  private def directoryExists(path: String): Boolean = {
    val finalPath = S3Utilities.prepareS3FilePath(path, Some(bucket))
    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withDelimiter("/")
      .withPrefix(if (!finalPath.endsWith("/")) finalPath + "/" else finalPath)
      .withMaxKeys(1)
    !s3Client.listObjectsV2(req).getObjectSummaries.isEmpty
  }
}

case class S3CopyInfo(s3Client: AmazonS3,
                      sourceBucket: String,
                      sourceKey: String,
                      destinationBucket: String,
                      destinationKey: String,
                      uploadId: String,
                      fileLength: Long)
