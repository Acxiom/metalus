package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.fs.FileManager
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
    "Copy source contents to destination",
    "Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy from")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy to"))))
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
    "Copy source contents to destination with buffering",
    "Copy the contents of the source path to the destination path using buffer sizes. This function will call connect on both FileManagers.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy from")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy to")),
    "inputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for reading data during copy")),
    "outputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for writing data during copy"))))
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
    "Buffered file copy",
    "Copy the contents of the source path to the destination path using full buffer sizes. This function will call connect on both FileManagers.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("srcFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The source FileManager")),
    "srcPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy from")),
    "destFS" -> StepParameter(None, Some(true), None, None, None, None, Some("The destination FileManager")),
    "destPath" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to copy to")),
    "inputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for reading data during copy")),
    "outputBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The size of the buffer to use for writing data during copy")),
    "copyBufferSize" -> StepParameter(None, Some(true), None, None, None, None, Some("The intermediate buffer size to use during copy"))))
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
    * Disconnects a FileManager from the underlying file system
    *
    * @param fileManager The FileManager implementation to disconnect
    */
  @StepFunction("3d1e8519-690c-55f0-bd05-1e7b97fb6633",
    "Disconnect a FileManager",
    "Disconnects a FileManager from the underlying file system",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("fileManager" -> StepParameter(None, Some(true), None, None, None, None, Some("The file manager to disconnect"))))
  def disconnectFileManager(fileManager: FileManager): Unit = {
    fileManager.disconnect()
  }
}

case class CopyResults(success: Boolean, fileSize: Long, durationMS: Long, startTime: Date, endTime: Date)
