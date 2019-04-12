package com.acxiom.pipeline.fs

import java.io.{FileNotFoundException, InputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

/**
  * HDFS based implementation of the FileManager.
  */
case class HDFSFileManager(sparkSession: SparkSession) extends FileManager {
  private val configuration = sparkSession.sparkContext.hadoopConfiguration
  private val fileSystem = FileSystem.get(configuration)

  override def exists(path: String): Boolean = fileSystem.exists(new Path(path))

  override def getInputStream(path: String, bufferSize: Int = DEFAULT_BUFFER_SIZE): InputStream = {
    val hdfsPath = new Path(path)
    if (!fileSystem.getFileStatus(hdfsPath).isDirectory) {
      fileSystem.open(new Path(path), bufferSize)
    } else {
      throw new IllegalArgumentException(s"Path is a directory, not a file,inputPath=$path")
    }
  }

  // TODO Look into better error handling
  override def getOutputStream(path: String, append: Boolean = true, bufferSize: Int = DEFAULT_BUFFER_SIZE): OutputStream =
    fileSystem.create(new Path(path), append, bufferSize)

  override def rename(path: String, destPath: String): Boolean = fileSystem.rename(new Path(path), new Path(destPath))

  override def deleteFile(path: String): Boolean = fileSystem.delete(new Path(path), true)

  /**
    * Connect to the file system
    */
  override def connect(): Unit = {
    // This implementation doesn't need to connect
  }

  /**
    * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
    *
    * @param path The path to the file
    * @return size of the given file
    */
  override def getSize(path: String): Long = {
    val hdfsPath = new Path(path)
    if(fileSystem.exists(hdfsPath)) {
      fileSystem.getContentSummary(hdfsPath).getLength
    } else {
      throw new FileNotFoundException(s"File not found when attempting to get size,inputPath=$path")
    }
  }

  /**
    * Disconnect from the file system
    */
  override def disconnect(): Unit = {
    // This implementation doesn't need to disconnect
  }

  /**
    * Returns a list of file names at the given path. An exception will be thrown if the path is invalid.
    *
    * @param path The path to list.
    * @return A list of files at the given path
    */
  override def getFileListing(path: String): List[FileInfo] = {
    val filePath = new Path(path)
    if (fileSystem.exists(filePath)) {
      iterateRemoteIterator(fileSystem.listFiles(filePath, false), List()).reverse
    } else {
      throw new FileNotFoundException(s"Path not found when attempting to get listing,inputPath=$path")
    }
  }

  @tailrec
  private def iterateRemoteIterator(iterator: RemoteIterator[LocatedFileStatus], list: List[FileInfo]): List[FileInfo] = {
    if (iterator.hasNext) {
      val status = iterator.next()
      iterateRemoteIterator(iterator, FileInfo(status.getPath.getName, status.getLen) :: list)
    } else {
      list
    }
  }
}