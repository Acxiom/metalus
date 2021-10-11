package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.fs.FileManager

/**
  * File connectors provide easily representable configuration for the various file systems. The connector
  * implementation provides a way to get the FileManager for that file system and can be used by steps.
  */
trait FileConnector extends Connector {
  /**
    * Creates and opens a FileManager.
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  def getFileManager(pipelineContext: PipelineContext): FileManager
}
