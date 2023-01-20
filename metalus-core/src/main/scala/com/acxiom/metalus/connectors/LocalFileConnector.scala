package com.acxiom.metalus.connectors

import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.fs.{FileManager, LocalFileManager}

/**
  * Provides access to the local file system. Credentials are not used by this connector.
  *
  * @param name           The name to provide this connector.
  * @param credentialName An unsused parameter.
  * @param credential     An unsused parameter.
  */
case class LocalFileConnector(override val name: String,
                              override val credentialName: Option[String],
                              override val credential: Option[Credential]) extends FileConnector {
  /**
    * Creates and opens a FileManager.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  override def getFileManager(pipelineContext: PipelineContext): FileManager = new LocalFileManager()
}
