package com.acxiom.gcp.pipeline.connectors

import com.acxiom.gcp.fs.GCSFileManager
import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.pipeline.connectors.FileConnector
import com.acxiom.pipeline.fs.FileManager
import com.acxiom.pipeline.{Credential, PipelineContext}

/**
  * Provides an implementation of FileConnector that works with GCS.
  *
  * @param projectId      The project id of the GCS project
  * @param bucket         The name of the GCS bucket
  * @param name           The name of this connector
  * @param credentialName The optional name of the credential to provide the CredentialProvider
  * @param credential     The optional credential to use. credentialName takes precedence if provided.
  */
case class GCSFileConnector(projectId: String,
                            bucket: String,
                            override val name: String,
                            override val credentialName: Option[String],
                            override val credential: Option[Credential]) extends FileConnector with GCSConnector {
  /**
    * Creates and opens a FileManager.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  override def getFileManager(pipelineContext: PipelineContext): FileManager = {
    val finalCredential = getCredential(pipelineContext)
    val jsonAuth = if (finalCredential.isDefined) {
      Some(new String(GCPUtilities.generateCredentialsByteArray(Some(finalCredential.get.authKey)).get))
    } else {
      None
    }
    new GCSFileManager(projectId, bucket, jsonAuth)
  }
}
