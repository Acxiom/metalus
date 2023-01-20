package com.acxiom.metalus.connectors

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.fs.FileManager

/**
  * File connectors provide easily representable configuration for the various file systems. The connector
  * implementation provides a way to get the FileManager for that file system and can be used by steps.
  */
trait FileConnector extends Connector {
  /**
    * Creates and opens a FileManager.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  def getFileManager(pipelineContext: PipelineContext): FileManager
}
