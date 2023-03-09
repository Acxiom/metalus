package com.acxiom.metalus.fs

import com.acxiom.metalus.utils.S3Utilities
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.io.{ByteArrayInputStream, OutputStream}
import scala.collection.mutable

class S3OutputStream(s3Client: S3Client, bucket: String, key: String, bufferLength: Option[Int] = None) extends OutputStream {
  private val bufferSize = bufferLength.getOrElse(S3Utilities.MULTIPART_UPLOAD_SIZE)
  private val request = CreateMultipartUploadRequest.builder()
    .bucket(bucket)
    .key(key)
    .build()
  private val response = s3Client.createMultipartUpload(request)
  private val uploadId = response.uploadId()

  // Reevaluate if there is a way to do this without mutable variables
  // These values are mutable because some state needed to be maintained to honor the interface
  private val parts = mutable.ListBuffer[CompletedPart]()
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
    val uploadRequest = UploadPartRequest.builder()
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .partNumber(partNumber).build()
    val etag = s3Client.uploadPart(uploadRequest,
      RequestBody.fromInputStream(new ByteArrayInputStream(buffer, 0, position), position)).eTag()
    parts += CompletedPart.builder().partNumber(1).eTag(etag).build()
    partNumber += 1
    position = 0
    buffer = new Array[Byte](bufferSize)
  }

  override def close(): Unit = {
    if (partNumber == 1 || position > 0) {
      writeBuffer()
    }
    val completedMultipartUpload = CompletedMultipartUpload.builder().parts(parts.toSeq: _*).build()
    val completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder.bucket(bucket).key(key)
      .uploadId(uploadId).multipartUpload(completedMultipartUpload).build

    s3Client.completeMultipartUpload(completeMultipartUploadRequest)
    super.close()
  }
}
