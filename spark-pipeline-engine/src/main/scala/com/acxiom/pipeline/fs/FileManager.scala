package com.acxiom.pipeline.fs

import java.io._

import org.apache.commons.io.FileUtils

object FileManager {
  def apply(): FileManager = new LocalFileManager
}

/**
  * The FileManager trait is an abstraction useful for working with files on different file systems such
  * as HDFS or local.
  */
trait FileManager {
  val DEFAULT_BUFFER_SIZE: Int = 4096
  /**
    * Connect to the file system
    */
  def connect(): Unit
  /**
    * Checks the path to determine whether it exists or not.
    *
    * @param path The path to verify
    * @return True if the path exists, otherwise false.
    */
  def exists(path: String): Boolean

  /**
    * Creates a buffered input stream for the provided path.
    *
    * @param path The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @return A buffered input stream
    */
  def getInputStream(path: String, bufferSize: Int = DEFAULT_BUFFER_SIZE): InputStream

  /**
    * Creates a buffered output stream for the provided path.
    *
    * @param path   The path where data will be written.
    * @param append Boolean flag indicating whether data should be appended. Default is true
    * @param bufferSize The buffer size to apply to the stream
    * @return
    */
  def getOutputStream(path: String, append: Boolean = true, bufferSize: Int = DEFAULT_BUFFER_SIZE): OutputStream

  /**
    * Will attempt to rename the provided path to the destination path.
    *
    * @param path     The path to rename.
    * @param destPath The destination path.
    * @return True if the path could be renamed.
    */
  def rename(path: String, destPath: String): Boolean

  /**
    * Attempts to delete the provided path.
    *
    * @param path The path to delete.
    * @return True if the path could be deleted.
    */
  def deleteFile(path: String): Boolean

  /**
    * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
    * @param path The path to the file
    * @return size of the given file
    */
  def getSize(path: String): Long

  /**
    * Returns a list of file names at the given path.
    * @param path The path to list.
    * @return A list of files at the given path
    */
  def getFileListing(path: String): List[FileInfo]

  /**
    * Disconnect from the file system
    */
  def disconnect(): Unit
}

case class FileInfo(fileName: String, size: Long)

/**
  * Default implementation of the FileManager that works with local files.
  */
class LocalFileManager extends FileManager {
  override def exists(path: String): Boolean = new File(path).exists()

  override def getInputStream(path: String, bufferSize: Int = DEFAULT_BUFFER_SIZE): InputStream = new BufferedInputStream(new FileInputStream(new File(path)))

  override def getOutputStream(path: String, append: Boolean = true, bufferSize: Int = DEFAULT_BUFFER_SIZE): OutputStream = {
    new BufferedOutputStream(new FileOutputStream(new File(path), append), bufferSize)
  }

  override def rename(path: String, destPath: String): Boolean = new File(path).renameTo(new File(destPath))

  override def deleteFile(path: String): Boolean = {
    val file = new File(path)
    if (file.isDirectory) {
      FileUtils.deleteDirectory(file)
      true
    } else {
      file.delete()
    }
  }

  /**
    * Connect to the file system
    */
  override def connect(): Unit = {
    // Not used
  }

  /**
    * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
    *
    * @param path The path to the file
    * @return size of the given file
    */
  override def getSize(path: String): Long = new File(path).length()

  /**
    * Returns a list of file names at the given path.
    *
    * @param path The path to list.
    * @return A list of files at the given path
    */
  override def getFileListing(path: String): List[FileInfo] =
    new File(path).listFiles().foldLeft(List[FileInfo]())((list, file) => FileInfo(file.getName, file.length()) :: list)

  /**
    * Disconnect from the file system
    */
  override def disconnect(): Unit = {
    // Not used
  }
}
