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
    srcFS.connect()
    destFS.connect()
    val inputStream = srcFS.getInputStream(srcPath)
    val outputStream = destFS.getOutputStream(destPath)
    val copied = destFS.copy(inputStream, outputStream)
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
