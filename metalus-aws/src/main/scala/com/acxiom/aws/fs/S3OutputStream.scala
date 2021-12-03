package com.acxiom.aws.fs

import com.acxiom.aws.utils.S3Utilities
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{CompleteMultipartUploadRequest, InitiateMultipartUploadRequest, PartETag, UploadPartRequest}

import java.io.{ByteArrayInputStream, OutputStream}
import scala.collection.JavaConverters._
import scala.collection.mutable

class S3OutputStream(s3Client: AmazonS3, bucket: String, key: String, bufferLength: Option[Int] = None) extends OutputStream {
  private val bufferSize = bufferLength.getOrElse(S3Utilities.MULTIPART_UPLOAD_SIZE)
  private val request = s3Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key))

  // Reevaluate if there is a way to do this without mutable variables
  // These values are mutable because some state needed to be maintained to honor the interface
  private val etags = mutable.ListBuffer[PartETag]()
  private var buffer = new Array[Byte](bufferSize)
  private var partNumber = 1
  private var position = 0

  override def write(b: Int): Unit = {
    buffer(position) = b.toByte
    position += 1
    if (position >= buffer.length) {
      writeBuffer()
    }
  }

  override def write(b: Array[Byte]): Unit = {
    write(b, 0, b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    b.slice(off, len).foreach(i => write(i.toInt))
  }

  private def writeBuffer(): Unit = {
    val uploadRequest = new UploadPartRequest()
      .withBucketName(bucket)
      .withKey(key)
      .withUploadId(request.getUploadId)
      .withPartNumber(partNumber)
      .withFileOffset(0)
      .withPartSize(position)
      .withInputStream(new ByteArrayInputStream(buffer, 0, position))
    val etag = s3Client.uploadPart(uploadRequest).getPartETag
    etags += etag
    partNumber += 1
    position = 0
    buffer = new Array[Byte](bufferSize)
  }

  override def close(): Unit = {
    if (buffer.nonEmpty || position > 0) {
      writeBuffer()
    }
    s3Client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, request.getUploadId, etags.toList.asJava))
    super.close()
  }
}
