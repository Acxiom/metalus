package com.acxiom.gcp.fs

import java.io._
import java.nio.channels.Channels

import com.acxiom.pipeline.fs.{FileInfo, FileManager}
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}

import scala.collection.JavaConverters._

class GCSFileManager(storage: Storage, bucket: String) extends FileManager {

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

  /**
    * Connect to the file system
    */
  override def connect(): Unit = {}

  /**
    * Checks the path to determine whether it exists or not.
    *
    * @param path The path to verify
    * @return True if the path exists, otherwise false.
    */
  override def exists(path: String): Boolean = {
    val blob = storage.get(bucket, path)
    Option(blob).isDefined && blob.exists()
  }

  /**
    * Creates a buffered input stream for the provided path.
    *
    * @param path       The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @return A buffered input stream
    */
  override def getInputStream(path: String, bufferSize: Int): InputStream = {
    new BufferedInputStream(Channels.newInputStream(storage.get(bucket, path).reader()), bufferSize)
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
    val blob = storage.get(bucket, path)
    if (Option(blob).isEmpty || !blob.exists()) {
      val newBlob = storage.create(BlobInfo.newBuilder(BlobId.of(bucket, path)).build())
      new BufferedOutputStream(Channels.newOutputStream(newBlob.writer()), bufferSize)
    } else {
      new BufferedOutputStream(Channels.newOutputStream(blob.writer()), bufferSize)
    }
  }

  /**
    * Will attempt to rename the provided path to the destination path.
    *
    * @param path     The path to rename.
    * @param destPath The destination path.
    * @return True if the path could be renamed.
    */
  override def rename(path: String, destPath: String): Boolean = {
    val blob = storage.get(bucket, path)
    val copyWriter = blob.copyTo(bucket, destPath)
    copyWriter.getResult
    blob.delete()
  }

  /**
    * Attempts to delete the provided path.
    *
    * @param path The path to delete.
    * @return True if the path could be deleted.
    */
  override def deleteFile(path: String): Boolean = {
    storage.get(bucket, path).delete()
  }

  /**
    * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
    *
    * @param path The path to the file.
    * @return size of the given file.
    */
  override def getSize(path: String): Long = {
    storage.get(bucket, path).getSize
  }

  /**
    * Returns a list of file names at the given path.
    *
    * @param path The path to list.
    * @return A list of files at the given path.
    */
  override def getFileListing(path: String): List[FileInfo] = {
    val page = storage.list(bucket, Storage.BlobListOption.prefix(path))
    page.iterateAll().iterator().asScala.foldLeft(List[FileInfo]())((list, blob) => {
      list :+ FileInfo(blob.getName, blob.getSize, blob.isDirectory)
    })
  }

  /**
    * Returns a list of directory names at the given path.
    *
    * @param path The path to list.
    * @return A list of directories at the given path.
    */
  override def getDirectoryListing(path: String): List[FileInfo] = {
    this.getFileListing(path).foldLeft(List[FileInfo]())((list, file) => {
      val index = file.fileName.lastIndexOf("/")
      if (index != -1) {
        val dirName = file.fileName.substring(0, index)
        if (!list.exists(_.fileName == dirName)) {
          list :+ FileInfo(dirName, 0L, directory = true)
        } else {
          list
        }
      } else {
        list
      }
    })
  }

  /**
    * Disconnect from the file system
    */
  override def disconnect(): Unit = {}
}
