package com.acxiom.metalus.gcp.fs

import com.acxiom.metalus.Constants
import com.acxiom.metalus.fs.{FileManager, FileResource}
import com.acxiom.metalus.gcp.utils.GCPUtilities.generateCredentialsByteArray
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}

import java.io._
import java.net.URI
import java.nio.channels.Channels
import scala.jdk.CollectionConverters._

object GCSFileManager {
  /**
   * This function will take the given path and strip any protocol information.
   *
   * @param path   A valid path
   * @param bucket An optional bucket name
   * @return A raw path with no protocol information
   */
  def prepareGCSFilePath(path: String, bucket: Option[String] = None): String = {
    val newPath = if (path.startsWith("/")) {
      path.substring(1)
    } else if (path.startsWith(s"gs:") && bucket.isDefined && path.contains(bucket.get)) {
      path.substring(path.indexOf(s"${bucket.get}/") + bucket.get.length + 1)
    } else if (path.startsWith(s"gs:") && bucket.isDefined) {
      path.substring(Constants.FIVE)
    } else if (path.startsWith(s"gs:")) {
      new URI(path).normalize().toString
    } else {
      path
    }
    newPath
  }
}

class GCSFileManager private[fs](storage: Storage, bucket: String) extends FileManager {
  def this(projectId: String, bucket: String, jsonAuth: Option[String]) = {
    this(
      (if (jsonAuth.isDefined) {
        StorageOptions.newBuilder.setCredentials(GoogleCredentials.fromStream(new ByteArrayInputStream(jsonAuth.get.getBytes))
          .createScoped("https://www.googleapis.com/auth/cloud-platform"))
      } else {
        StorageOptions.newBuilder
      }).setProjectId(projectId).build.getService,
      bucket)
  }

  def this(projectId: String, bucket: String, credentials: Map[String, String]) = {
    this(projectId, bucket, generateCredentialsByteArray(Some(credentials)).map(new String(_)))
  }

  def this(bucket: String, credentials: Map[String, String]) = {
    this(credentials("project_id"), bucket, credentials)
  }

  /**
   * Connect to the file system
   */
  override def connect(): Unit = {
    // Not used by GCS
  }

  /**
   * Checks the path to determine whether it exists or not.
   *
   * @param path The path to verify
   * @return True if the path exists, otherwise false.
   */
  override def exists(path: String): Boolean = fileExists(path) || directoryExists(path)

  /**
   * Returns a list of file names at the given path.
   *
   * @param path      The path to list.
   * @param recursive Flag indicating whether to run a recursive or simple listing.
   * @return A list of files at the given path.
   */
  override def getFileListing(path: String, recursive: Boolean = true): List[FileResource] = {
    if (!recursive) {
      storage.list(bucket, Storage.BlobListOption.delimiter("/"),
        Storage.BlobListOption.prefix(GCSFileManager.prepareGCSFilePath(path, Some(bucket))))
        .iterateAll().iterator().asScala.foldLeft(List[FileResource]())((list, blob) => {
        list :+ GCSFileResource(storage, bucket, blob, path)
      }).filterNot(_.directory)
    } else {
      val page = storage.list(bucket, Storage.BlobListOption.prefix(GCSFileManager.prepareGCSFilePath(path, Some(bucket))))
      page.iterateAll().iterator().asScala.foldLeft(List[FileResource]())((list, blob) => {
        list :+ GCSFileResource(storage, bucket, blob, path)
      })
    }
  }

  /**
   * Returns a list of directory names at the given path.
   *
   * @param path The path to list.
   * @return A list of directories at the given path.
   */
  override def getDirectoryListing(path: String): List[FileResource] = {
    this.getFileListing(GCSFileManager.prepareGCSFilePath(path, Some(bucket))).foldLeft(List[FileResource]())((list, file) => {
      val index = file.fullName.lastIndexOf("/")
      if (index != -1) {
        val dirName = file.fullName.substring(0, index)
        if (!list.exists(_.fullName == dirName)) {
          list :+ GCSFileResource(storage, bucket, storage.get(BlobId.of(bucket, dirName)), dirName)
        } else {
          list
        }
      } else {
        list
      }
    })
  }

  private def directoryExists(path: String): Boolean = {
    val finalPath = GCSFileManager.prepareGCSFilePath(path, Some(bucket))
    storage.list(
      bucket,
      Storage.BlobListOption.delimiter("/"),
      Storage.BlobListOption.prefix(finalPath),
      Storage.BlobListOption.pageSize(1)
    ).getValues.iterator().hasNext
  }

  private def fileExists(path: String): Boolean = {
    val blob = storage.get(bucket, GCSFileManager.prepareGCSFilePath(path, Some(bucket)))
    Option(blob).isDefined && blob.exists()
  }

  /**
   * Disconnect from the file system
   */
  override def disconnect(): Unit = {
    // Not used by GCS
  }

  override def getFileResource(path: String): FileResource = {
    val finalPath = GCSFileManager.prepareGCSFilePath(path, Some(bucket))
    val blob = storage.get(BlobId.of(bucket, finalPath))
    GCSFileResource(storage, bucket, blob, finalPath)
  }
}

private case class GCSFileResource(storage: Storage, bucket: String, blob: Blob, path: String) extends FileResource {
  override def fileName: String = (if (Option(blob).isDefined) blob.getName else path).split('/').last

  override def fullName: String = if (Option(blob).isDefined) blob.getName else path

  override def directory: Boolean = if (Option(blob).isDefined) blob.isDirectory else true

  override def rename(destPath: String): Boolean = {
    val copyWriter = blob.copyTo(bucket, destPath)
    copyWriter.getResult
    blob.delete()
  }

  override def delete: Boolean = blob.delete()

  override def size: Long = if (Option(blob).isDefined) {
    blob.getSize
  }  else {
    throw new FileNotFoundException(s"File not found when attempting to get size,inputPath=$fullName")
  }

  override def getInputStream(bufferSize: Int): InputStream =
    new BufferedInputStream(Channels.newInputStream(blob.reader()), bufferSize)

  override def getOutputStream(append: Boolean, bufferSize: Int): OutputStream = {
    if (Option(blob).isEmpty || !blob.exists()) {
      val newBlob = storage.create(BlobInfo.newBuilder(BlobId.of(bucket, path)).build())
      new BufferedOutputStream(Channels.newOutputStream(newBlob.writer()), bufferSize)
    } else {
      new BufferedOutputStream(Channels.newOutputStream(blob.writer()), bufferSize)
    }
  }
}
