package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.fs.FileManager

@StepObject
object FileManagerSteps {
  /**
    * Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.
    *
    * @param srcFS    FileManager for the source file system
    * @param srcPath  Source path
    * @param destFS   FileManager for the destination file system
    * @param destPath Destination path
    * @return True if the contents were copied.
    */
  @StepFunction("0342654c-2722-56fe-ba22-e342169545af",
    "Copy source contents to destination",
    "Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.",
    "Pipeline",
    "InputOutput")
  def copy(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String): Boolean = {
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
    * @return True if the contents were copied.
    */
  @StepFunction("c40169a3-1e77-51ab-9e0a-3f24fb98beef",
    "Copy source contents to destination with buffering",
    "Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.",
    "Pipeline",
    "InputOutput")
  def copy(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String, inputBufferSize: Int, outputBufferSize: Int): Boolean = {
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
    * @return True if the contents were copied.
    */
  @StepFunction("c40169a3-1e77-51ab-9e0a-3f24fb98beef",
    "Copy source contents to destination with buffering",
    "Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.",
    "Pipeline",
    "InputOutput")
  def copy(srcFS: FileManager, srcPath: String, destFS: FileManager, destPath: String,
           inputBufferSize: Int, outputBufferSize: Int, copyBufferSize: Int): Boolean = {
    srcFS.connect()
    destFS.connect()
    val inputStream = srcFS.getInputStream(srcPath, inputBufferSize)
    val outputStream = destFS.getOutputStream(destPath, bufferSize = outputBufferSize)
    val copied = destFS.copy(inputStream, outputStream, copyBufferSize)
    inputStream.close()
    outputStream.close()
    copied
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
  def disconnectFileManager(fileManager: FileManager): Unit = {
    fileManager.disconnect()
  }
}
