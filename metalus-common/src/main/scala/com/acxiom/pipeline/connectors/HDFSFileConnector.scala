package com.acxiom.pipeline.connectors
import com.acxiom.pipeline.fs.{FileManager, HDFSFileManager}
import com.acxiom.pipeline.{Credential, PipelineContext}

/**
  * Provides a FileConnector for the HDFS file system
  */
case class HDFSFileConnector(override val name: String,
                             override val credentialName: Option[String],
                             override val credential: Option[Credential]) extends FileConnector {
  /**
    * Creates and opens a FileManager.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  override def getFileManager(pipelineContext: PipelineContext): FileManager = {
    HDFSFileManager(pipelineContext.sparkSession.get.sparkContext.getConf)
  }
}
