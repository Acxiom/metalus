package com.acxiom.metalus.spark.fs

import com.acxiom.metalus.fs.{FileManager, FileResource}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.MetalusSparkHadoopUtil

import java.io.{FileNotFoundException, InputStream, OutputStream}

case class HDFSFileManager(conf: SparkConf) extends FileManager {
  private val configuration = MetalusSparkHadoopUtil.newConfiguration(conf)

  private val fileSystem = FileSystem.get(configuration)

  override def connect(): Unit = {
    // This implementation doesn't need to connect
  }

  override def exists(path: String): Boolean = fileSystem.exists(new Path(path))

  override def getFileListing(path: String, recursive: Boolean): List[FileResource] = {
    val filePath = new Path(path)
    if (fileSystem.exists(filePath)) {
      parseListing(fileSystem.listFiles(filePath, recursive))
    } else {
      throw new FileNotFoundException(s"Path not found when attempting to get listing,inputPath=$path")
    }
  }

  override def getDirectoryListing(path: String): List[FileResource] = {
    val filePath = new Path(path)
    if (fileSystem.exists(filePath)) {
      parseListing(fileSystem.listLocatedStatus(filePath)).filter(_.directory)
    } else {
      throw new FileNotFoundException(s"Path not found when attempting to get listing,inputPath=$path")
    }
  }

  private def parseListing(rs: RemoteIterator[LocatedFileStatus]) = {
    Iterator.from(0).takeWhile(_ => rs.hasNext).map(_ => {
      val status = rs.next()
      HDFSFileResource(status.getPath, fileSystem)
    }).toList
  }

  override def getFileResource(path: String): FileResource = HDFSFileResource(new Path(path), fileSystem)

  override def disconnect(): Unit = {
    // This implementation doesn't need to disconnect
  }
}

case class HDFSFileResource(filePath: Path, fileSystem: FileSystem) extends FileResource {
  override def fileName: String = filePath.getName

  override def fullName: String = filePath.toUri.getPath

  override def directory: Boolean = fileSystem.getFileStatus(filePath).isDirectory

  override def rename(destPath: String): Boolean = fileSystem.rename(filePath, new Path(destPath))

  override def delete: Boolean = fileSystem.delete(filePath, true)

  override def size: Long = {
    if (fileSystem.exists(filePath)) {
      fileSystem.getContentSummary(filePath).getLength
    } else {
      throw new FileNotFoundException(s"File not found when attempting to get size,inputPath=${filePath.toString}")
    }
  }

  override def getInputStream(bufferSize: Int): InputStream = {
    if (!fileSystem.getFileStatus(filePath).isDirectory) {
      fileSystem.open(filePath, bufferSize)
    } else {
      throw new IllegalArgumentException(s"Path is a directory, not a file,inputPath=${filePath.toString}")
    }
  }

  override def getOutputStream(append: Boolean, bufferSize: Int): OutputStream =
    fileSystem.create(filePath, append, bufferSize)
}
