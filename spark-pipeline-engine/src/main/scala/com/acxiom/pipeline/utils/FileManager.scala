package com.acxiom.pipeline.utils

import java.io._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object FileManager {
  def apply(): FileManager = new LocalFileManager
}

/**
  * The FileManager trait is an abstraction useful for working with files on different file systems such
  * as HDFS or local.
  */
trait FileManager {
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
    * @return A buffered input stream
    */
  def getInputStream(path: String): InputStream

  /**
    * Creates a buffered output stream for the provided path.
    *
    * @param path   The path where data will be written.
    * @param append Boolean flag indicating whether data should be appended. Default is true
    * @return
    */
  def getOutputStream(path: String, append: Boolean = true): OutputStream

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
}

/**
  * Default implementation of the FileManager that works with local files.
  */
class LocalFileManager extends FileManager {
  override def exists(path: String): Boolean = new File(path).exists()

  override def getInputStream(path: String): InputStream = new BufferedInputStream(new FileInputStream(new File(path)))

  override def getOutputStream(path: String, append: Boolean = true): OutputStream = {
    new BufferedOutputStream(new FileOutputStream(new File(path), append))
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
}

/**
  * HDFS based implementation of the FIleManager.
  */
class HDFSFileManager(sparkSession: SparkSession) extends FileManager {
  private val hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration
  private val fileSystem = FileSystem.get(hadoopConfiguration)

  override def exists(path: String): Boolean = fileSystem.exists(new Path(path))

  override def getInputStream(path: String): InputStream = fileSystem.open(new Path(path))

  override def getOutputStream(path: String, append: Boolean = true): OutputStream = fileSystem.create(new Path(path), append)

  override def rename(path: String, destPath: String): Boolean = fileSystem.rename(new Path(path), new Path(destPath))

  override def deleteFile(path: String): Boolean = fileSystem.delete(new Path(path), true)
}
