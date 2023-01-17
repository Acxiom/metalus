package com.acxiom.pipeline.fs

import org.apache.log4j.Logger

import java.io._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

object FileManager {
  val DEFAULT_BUFFER_SIZE: Int = 65536
  val DEFAULT_COPY_BUFFER_SIZE: Int = 32768
  def apply(): FileManager = new LocalFileManager
}

/**
  * The FileManager trait is an abstraction useful for working with files on different file systems such
  * as HDFS or local.
  */
trait FileManager {
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
    * Returns a list of file names at the given path.
    * @param path The path to list.
    * @param recursive Flag indicating whether to run a recursive or simple listing.
    * @return A list of files at the given path.
    */
  def getFileListing(path: String, recursive: Boolean = false): List[FileResource]

  /**
   * Returns a list of directory names at the given path.
   * @param path The path to list.
   * @return A list of directories at the given path.
   */
  def getDirectoryListing(path: String): List[FileResource] = getFileListing(path).filter(_.directory)

  /**
   * Returns a FileResource object for the given path
   * @param path The path to get the FileResource.
   * @return A FileResource object for the path given.
   */
  def getFileResource(path: String): FileResource

  /**
    * Disconnect from the file system
    */
  def disconnect(): Unit
}

trait FileResource {
  protected val logger: Logger = Logger.getLogger(getClass)

  /**
   * The simple name of this file. Does not include the full path.
   *
   * @return The file name.
   */
  def fileName: String

  /**
   * Returns the full name of this file including path.
   *
   * @return The full name.
   */
  def fullName: String

  /**
   * True if this represents a directory.
   *
   * @return True if this represents a directory.
   */
  def directory: Boolean

  /**
   * Will attempt to rename this file to the destination path.
   *
   * @param destPath The destination path.
   * @return True if the path could be renamed.
   */
  def rename(destPath: String): Boolean

  /**
   * Attempts to delete this file.
   *
   * @return True if the path could be deleted.
   */
  def delete: Boolean

  /**
   * Get the size of this file.
   *
   * @return size of this file.
   */
  def size: Long

  /**
   * Creates a buffered input stream for this file.
   *
   * @param bufferSize The buffer size to apply to the stream
   * @return A buffered input stream
   */
  def getInputStream(bufferSize: Int = FileManager.DEFAULT_BUFFER_SIZE): InputStream

  /**
   * Creates a buffered output stream for this file.
   *
   * @param append     Boolean flag indicating whether data should be appended. Default is true
   * @param bufferSize The buffer size to apply to the stream
   * @return
   */
  def getOutputStream(append: Boolean = true, bufferSize: Int = FileManager.DEFAULT_BUFFER_SIZE): OutputStream

  /**
   * Copies all of the contents of this file to the destination file. The base implementation will
   * use streams to copy the data.
   *
   * @param destination A FileResource representing the destination of the copy.
   * @return True if the copy was successful
   */
  def copy(destination: FileResource): Boolean = {
    copy(destination, FileManager.DEFAULT_COPY_BUFFER_SIZE)
  }

  /**
   * Copies all of the contents of this file to the destination file. The base implementation will
   * use streams to copy the data.
   *
   * @param destination    A FileResource representing the destination of the copy.
   * @param copyBufferSize The size in bytes of the copy buffer.
   * @return True if the copy was successful.
   */
  def copy(destination: FileResource, copyBufferSize: Int, closeStreams: Boolean = false): Boolean = {
    try {
      val buffer = new Array[Byte](copyBufferSize)
      val input = this.getInputStream()
      val output = destination.getOutputStream()
      Stream.continually(input.read(buffer)).takeWhile(_ != -1).foreach(count => {
        output.write(buffer, 0, count)
      })
      output.flush()
      if (closeStreams) {
        try {
          input.close()
        }
        try {
          output.close()
        }
      }
      true
    } catch {
      case t: Throwable =>
        logger.error("Unable to perform copy operation", t)
        false
    }
  }
}

case class LocalFileResource(file: File) extends FileResource {
  /**
   * The simple name of this file. Does not include the full path.
   *
   * @return The file name.
   */
  override def fileName: String = file.getName

  /**
   * Returns the full name of this file including path.
   *
   * @return The full name.
   */
  override def fullName: String = file.getAbsolutePath

  /**
   * True if this represents a directory.
   *
   * @return True if this represents a directory.
   */
  override def directory: Boolean = file.isDirectory

  /**
   * Will attempt to rename this file to the destination path.
   *
   * @param destPath The destination path.
   * @return True if the path could be renamed.
   */
  override def rename(destPath: String): Boolean = file.renameTo(new File(destPath))

  /**
   * Attempts to delete this file.
   *
   * @return True if the path could be deleted.
   */
  override def delete: Boolean = {
    if (file.isDirectory) {
      deleteNio()
      true
    } else {
      file.delete()
    }
  }

  /**
   * Get the size of this file.
   *
   * @return size of this file.
   */
  override def size: Long = file.length()

  /**
   * Creates a buffered input stream for this file.
   *
   * @param bufferSize The buffer size to apply to the stream
   * @return A buffered input stream
   */
  override def getInputStream(bufferSize: Int): InputStream =
    new BufferedInputStream(new FileInputStream(file))

  /**
   * Creates a buffered output stream for this file.
   *
   * @param append     Boolean flag indicating whether data should be appended. Default is true
   * @param bufferSize The buffer size to apply to the stream
   * @return
   */
  override def getOutputStream(append: Boolean = true, bufferSize: Int = FileManager.DEFAULT_BUFFER_SIZE): OutputStream =
        new BufferedOutputStream(new FileOutputStream(file, append), bufferSize)

  override def copy(destination: FileResource): Boolean = {
    destination match {
      case resource: LocalFileResource => Files.copy(file.toPath, resource.file.toPath).toFile.exists()
      case _ => super.copy(destination)
    }
  }
  private def deleteNio(): Unit = {
    Files.walkFileTree(file.toPath,
      new SimpleFileVisitor[Path] {
        override def visitFile(file: Path,
                                attrs: BasicFileAttributes
                              ): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path,
                                         exc: IOException
                                       ): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )
  }
}

/**
  * Default implementation of the FileManager that works with local files.
  */
class LocalFileManager extends FileManager {
  override def exists(path: String): Boolean = new File(path).exists()

  /**
    * Connect to the file system
    */
  override def connect(): Unit = {
    // Not used
  }

  /**
    * Returns a list of file names at the given path.
    *
    * @param path The path to list.
    * @param recursive Flag indicating whether to run a recursive or simple listing.
    * @return A list of files at the given path
    */
  override def getFileListing(path: String, recursive: Boolean = false): List[FileResource] = {
    if (recursive) {
      new File(path).listFiles().foldLeft(List[FileResource]()) {
        case (listing, file) if file.isDirectory && (file.getName == "." || file.getName == "..") => listing
        case (listing, file) if file.isDirectory => listing ++ getFileListing(file.getAbsolutePath, recursive)
        case (listing, file) => listing :+ LocalFileResource(file)
      }
    } else {
      new File(path)
        .listFiles()
        .foldLeft(List[FileResource]()){ (list, file) => LocalFileResource(file) :: list
        }
    }
  }

  /**
   * Returns a FileInfo objects for the given path
   * @param path The path to get a status of.
   * @return A FileInfo object for the path given.
   */
  override def getFileResource(path: String): FileResource = {
    val file = new File(path)
    LocalFileResource(file)
  }

  /**
    * Disconnect from the file system
    */
  override def disconnect(): Unit = {
    // Not used
  }
}
