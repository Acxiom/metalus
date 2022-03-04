package com.acxiom.pipeline.fs

import java.io.{FileNotFoundException, InputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.MetalusSparkHadoopUtil

import scala.annotation.tailrec

/**
  * HDFS based implementation of the FileManager.
  */
case class HDFSFileManager(conf: SparkConf) extends FileManager {
  private val configuration = MetalusSparkHadoopUtil.newConfiguration(conf)
  private val fileSystem = FileSystem.get(configuration)

  override def exists(path: String): Boolean = fileSystem.exists(new Path(path))

  override def getInputStream(path: String, bufferSize: Int = FileManager.DEFAULT_BUFFER_SIZE): InputStream = {
    val hdfsPath = new Path(path)
    if (!fileSystem.getFileStatus(hdfsPath).isDirectory) {
      fileSystem.open(new Path(path), bufferSize)
    } else {
      throw new IllegalArgumentException(s"Path is a directory, not a file,inputPath=$path")
    }
  }

  override def getOutputStream(path: String, append: Boolean = true, bufferSize: Int = FileManager.DEFAULT_BUFFER_SIZE): OutputStream =
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
   * Directories will not be included in this listing.
   *
   * @param path The path to list.
   * @param recursive Flag indicating whether to run a recursive or simple listing.
   * @return A list of files at the given path
   */
  override def getFileListing(path: String, recursive: Boolean = false): List[FileInfo] = {
    val filePath = new Path(path)
    if (fileSystem.exists(filePath)) {
      iterateRemoteIterator(fileSystem.listFiles(filePath, recursive), List()).reverse
    } else {
      throw new FileNotFoundException(s"Path not found when attempting to get listing,inputPath=$path")
    }
  }

  /**
   * Returns a list of directory names at the given path. An exception will be thrown if the path is invalid.
   *
   * @param path The path to list.
   * @return A list of directories at the given path.
   */
  override def getDirectoryListing(path: String): List[FileInfo] = {
    val filePath = new Path(path)
    if (fileSystem.exists(filePath)) {
      fileSystem.listStatus(filePath)
        .filter(_.isDirectory)
        .map(s => FileInfo(s.getPath.getName, s.getLen, s.isDirectory))
        .toList
    } else {
      throw new FileNotFoundException(s"Path not found when attempting to get listing,inputPath=$path")
    }
  }

  /**
   * Returns a FileInfo objects for the given path
   * @param path The path to get a status of.
   * @return A FileInfo object for the path given.
   */
  override def getStatus(path: String): FileInfo = {
    val fs = fileSystem.getFileStatus(new Path(path))
    FileInfo(fs.getPath.getName, fs.getLen, fs.isDirectory, Option(fs.getPath.getParent).map(_.toString))
  }

  @tailrec
  private def iterateRemoteIterator(iterator: RemoteIterator[LocatedFileStatus], list: List[FileInfo]): List[FileInfo] = {
    if (iterator.hasNext) {
      val status = iterator.next()
      iterateRemoteIterator(iterator, FileInfo(status.getPath.getName, status.getLen, status.isDirectory,
        Option(status.getPath.getParent).map(_.toString)) :: list)
    } else {
      list
    }
  }
}
