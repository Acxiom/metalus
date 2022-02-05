package com.acxiom.pipeline.steps

import java.io.{InputStream, OutputStream}

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.connectors.FileConnector
import com.acxiom.pipeline.fs.{FileInfo, FileManager}
import org.apache.log4j.Logger
import java.util.Date

@StepObject
object FileManagerSteps {
  private val logger = Logger.getLogger(getClass)

  /**
    * Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.
    *
    * @param srcFS    FileManager for the source file system
    * @param srcPath  Source path
    * @param destFS   FileManager for the destination file system
    * @param destPath Destination path
    * @return object with copy results.
    */
  @StepFunction("0342654c-2722-56fe-ba22-e342169545af",
    "Copy (auto buffering)",
    "Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.",
    "Pipeline",
    "FileManager")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy from")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy to"))))
  @StepResults(primaryType = "com.acxiom.pipeline.steps.CopyResults",
    secondaryTypes = None)
  def copy(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String): CopyResults = {
    copy(srcFS, srcPath, destFS, destPath, FileManager.DEFAULT_BUFFER_SIZE, FileManager.DEFAULT_BUFFER_SIZE)
  }

  /**
    * Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.
    *
    * @param srcFS    FileManager for the source file system
    * @param srcPath  Source path
    * @param destFS   FileManager for the destination file system
    * @param destPath Destination path
    * @param inputBufferSize The size of the buffer for the input stream
    * @param outputBufferSize The size of the buffer for the output stream
    * @return object with copy results.
    */
  @StepFunction("c40169a3-1e77-51ab-9e0a-3f24fb98beef",
    "Copy (basic buffering)",
    "Copy the contents of the source path to the destination path using buffer sizes. This function will call connect on both FileManagers.",
    "Pipeline",
    "FileManager")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy from")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy to")),
    "inputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for reading data during copy")),
    "outputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for writing data during copy"))))
  @StepResults(primaryType = "com.acxiom.pipeline.steps.CopyResults",
    secondaryTypes = None)
  def copy(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String, inputBufferSize: Int, outputBufferSize: Int): CopyResults = {
    copy(srcFS, srcPath, destFS, destPath, inputBufferSize, outputBufferSize, FileManager.DEFAULT_COPY_BUFFER_SIZE)
  }

  /**
    * Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.
    *
    * @param srcFS    FileManager for the source file system
    * @param srcPath  Source path
    * @param destFS   FileManager for the destination file system
    * @param destPath Destination path
    * @param inputBufferSize The size of the buffer for the input stream
    * @param outputBufferSize The size of the buffer for the output stream
    * @param copyBufferSize The size of the buffer used to transfer from input to output
    * @return object with copy results.
    */
  @StepFunction("f5a24db0-e91b-5c88-8e67-ab5cff09c883",
    "Copy (advanced buffering)",
    "Copy the contents of the source path to the destination path using full buffer sizes. This function will call connect on both FileManagers.",
    "Pipeline",
    "FileManager")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy from")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy to")),
    "inputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for reading data during copy")),
    "outputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for writing data during copy")),
    "copyBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The intermediate buffer size to use during copy"))))
  @StepResults(primaryType = "com.acxiom.pipeline.steps.CopyResults",
    secondaryTypes = None)
  def copy(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String,
           inputBufferSize: Int, outputBufferSize: Int, copyBufferSize: Int): CopyResults = {
    // connect to source and destination file systems
    srcFS.connect()
    destFS.connect()

    // create input and output streams
    val inputStream = srcFS.getInputStream(srcPath, inputBufferSize)
    val outputStream = destFS.getOutputStream(destPath, bufferSize = outputBufferSize)
    logger.info(s"starting copy,source=$srcPath,destination=$destPath")

    // track size and start time
    val size = srcFS.getSize(srcPath)
    val startTime = new Date()

    // start the copy
    val copied = destFS.copy(inputStream, outputStream, copyBufferSize)
    val endTime = new Date()
    val duration = endTime.getTime - startTime.getTime
    logger.info(s"copy complete,success=$copied,size=$size,durationMS=$duration")

    // close input and output streams
    inputStream.close()
    outputStream.close()

    // return metrics
    CopyResults(copied, size, duration, startTime, endTime)
  }

  /**
    * Verify that a source path and destination path are the same size.
    *
    * @param srcFS    FileManager for the source file system
    * @param srcPath  Source path
    * @param destFS   FileManager for the destination file system
    * @param destPath Destination path
    * @return true if the source and destination files are the same size
    */
  @StepFunction("1af68ab5-a3fe-4afb-b5fa-34e52f7c77f5",
    "Compare File Sizes",
    "Compare the file sizes of the source and destination paths",
    "Pipeline",
    "FileManager")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to the source")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to th destination"))))
  def compareFileSizes(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String): Int =
    srcFS.getSize(srcPath).compareTo(destFS.getSize(destPath))

  /**
    * Delete the file using the provided FileManager and Path
    *
    * @param fileManager The FileManager to use when deleting the file
    * @param path        The full path to the file
    * @return true if the file can be deleted
    */
  @StepFunction("bf2c4df8-a215-480b-87d8-586984e04189",
    "Delete (file)",
    "Delete a file",
    "Pipeline",
    "FileManager")
  @StepParameters(Map("fileManager" -> StepParameter(None, Some(true), None, None, None, None, Some("The FileManager")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to the file being deleted"))))
  @StepResults(primaryType = "Boolean", secondaryTypes = None)
  def deleteFile(fileManager: FileManager, path: String): Boolean =
    fileManager.deleteFile(path)

  /**
    * Disconnects a FileManager from the underlying file system
    *
    * @param fileManager The FileManager implementation to disconnect
    */
  @StepFunction("3d1e8519-690c-55f0-bd05-1e7b97fb6633",
    "Disconnect a FileManager",
    "Disconnects a FileManager from the underlying file system",
    "Pipeline",
    "FileManager")
  @StepParameters(Map("fileManager" -> StepParameter(None, Some(true), None, None, None, None, Some("The file manager to disconnect"))))
  def disconnectFileManager(fileManager: FileManager): Unit = {
    fileManager.disconnect()
  }

  /**
    * Creates a FileManager from provided connector
    *
    * @return fileManager The FileManager implementation
    */
  @StepFunction("259a880a-3e12-4843-9f02-2cfc2a05f576",
    "Create a FileManager",
    "Creates a FileManager using the provided FileConnector",
    "Pipeline",
    "Connectors")
  @StepParameters(Map("fileConnector" -> StepParameter(None, Some(true), None, None,
    None, None, Some("The FileConnector to use to create the FileManager implementation"))))
  def getFileManager(fileConnector: FileConnector, pipelineContext: PipelineContext): FileManager =
    fileConnector.getFileManager(pipelineContext)

  @StepFunction("5d59b2e8-7f58-4055-bf82-0d17c5a79a17",
    "Get an InputStream",
    "Gets an InputStream using the provided FileManager",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use to get the InputStream")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file being read")),
    "bufferSize" -> StepParameter(None, Some(false), None, None,
      None, None, Some("The buffer size to use for the InputStream"))
  ))
  def getInputStream(fileManager: FileManager, path: String, bufferSize: Option[Int] = None): InputStream =
    bufferSize.map(fileManager.getInputStream(path, _)).getOrElse(fileManager.getInputStream(path))

  @StepFunction("89eee531-4eb7-4059-9ad3-99a33d252069",
    "Get an OutputStream",
    "Gets an OutputStream using the provided FileManager",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use to get the OutputStream")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file being read")),
    "append" -> StepParameter(None, Some(false), None, None,
      None, None, Some("Should the OutputStream append to an existing file")),
    "bufferSize" -> StepParameter(None, Some(false), None, None,
      None, None, Some("The buffer size to use for the OutputStream"))
  ))
  def getOutputStream(fileManager: FileManager, path: String, append: Option[Boolean] = None,
                      bufferSize: Option[Int] = None): OutputStream = (append, bufferSize) match {
    case (Some(a), Some(b)) =>
      fileManager.getOutputStream(path, a, b)
    case (Some(a), None) => fileManager.getOutputStream(path, a)
    case (None, Some(b)) => fileManager.getOutputStream(path, bufferSize = b)
    case _ => fileManager.getOutputStream(path)
  }

  @StepFunction("22c4cc61-1cd8-4ee2-8589-d434d8854c55",
    "Rename a File",
    "Renames a file using the provided FileManager",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use for the rename operation")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file being renamed")),
    "destPath" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The destination of the file"))
  ))
  def rename(fileManager: FileManager, path: String, destPath: String): Boolean = fileManager.rename(path, destPath)

  @StepFunction("b38f857b-aa37-440a-8824-659fae60a0df",
    "Get File Size",
    "Gets the size of a file",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use for the size operation")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file"))
  ))
  def getSize(fileManager: FileManager, path: String): Long = fileManager.getSize(path)

  @StepFunction("aec5ebf7-7dac-4132-8d58-3a06b4772f79",
    "Does File Exist",
    "Checks whether a file exists",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use for the size operation")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file"))
  ))
  def exists(fileManager: FileManager, path: String): Boolean = fileManager.exists(path)

  @StepFunction("71ff49ef-1256-415f-b5d6-06aaf2f5dde1",
    "Get a File Listing",
    "Gets a file listing using the provided FileManager",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use for the rename operation")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file being renamed")),
    "recursive" -> StepParameter(None, Some(false), None, None,
      None, None, Some("Should the listing be recursive"))
  ))
  def getFileListing(fileManager: FileManager, path: String, recursive: Option[Boolean] = None): List[FileInfo] =
    recursive.map(fileManager.getFileListing(path, _)).getOrElse(fileManager.getFileListing(path))

  @StepFunction("c941f117-85a1-4793-9c4d-fdd986797979",
    "Get a Directory Listing",
    "Gets a directory listing using the provided FileManager",
    "Pipeline",
    "Connectors")
  @StepParameters(Map(
    "fileManager" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The FileManager to use for the rename operation")),
    "path" -> StepParameter(None, Some(true), None, None,
      None, None, Some("The path of the file being renamed"))
  ))
  def getDirectoryListing(fileManager: FileManager, path: String): List[FileInfo] = fileManager.getDirectoryListing(path)
}

case class CopyResults(success: Boolean, fileSize: Long, durationMS: Long, startTime: Date, endTime: Date)
