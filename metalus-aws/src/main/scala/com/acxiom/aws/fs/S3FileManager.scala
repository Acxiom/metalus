package com.acxiom.aws.fs

import java.io.{InputStream, OutputStream}

import com.acxiom.aws.utils.S3Utilities
import com.acxiom.pipeline.fs.{FileInfo, FileManager}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class S3FileManager(region: String,
                    bucket: String,
                    accessKeyId: Option[String] = None,
                    secretAccessKey: Option[String] = None) extends FileManager {
  private lazy val credentials = if (accessKeyId.isDefined && secretAccessKey.isDefined) {
    new BasicAWSCredentials(accessKeyId.get, secretAccessKey.get)
  } else {
    new AnonymousAWSCredentials()
  }
  private lazy val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withRegion(region)
      .build()

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
    s3Client.doesObjectExist(bucket, S3Utilities.prepareS3FilePath(path, Some(bucket)))
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

    s3Client.deleteObject(bucket, path)
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
    // get a list of all objects with this prefix (in case path is a folder)
    val s3ObjectList = s3Client.listObjects(bucket, S3Utilities.prepareS3FilePath(path, Some(bucket))).getObjectSummaries

    // convert to Scala list and sum the size of all objects with the prefix
    s3ObjectList.asScala.map(_.getSize).sum
  }

  /**
    * Returns a list of file names at the given path.
    *
    * @param path The path to list.
    * @return A list of files at the given path
    */
  override def getFileListing(path: String): List[FileInfo] = {
    nextObjectBatch(s3Client, s3Client.listObjects(bucket, path))
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
}

case class S3CopyInfo(s3Client: AmazonS3,
                      sourceBucket: String,
                      sourceKey: String,
                      destinationBucket: String,
                      destinationKey: String,
                      uploadId: String,
                      fileLength: Long)
