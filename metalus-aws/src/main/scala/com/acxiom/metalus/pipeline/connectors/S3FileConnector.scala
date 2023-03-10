package com.acxiom.metalus.pipeline.connectors

import com.acxiom.metalus.connectors.FileConnector
import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.fs.{FileManager, S3FileManager}

/**
  * Provides an implementation of theFileConnector that works with S3.
  *
  * @param region         The AWS region
  * @param bucket         The S3 bucket
  * @param name           The name of this connector
  * @param credentialName The optional name of the credential to provide the CredentialProvider
  * @param credential     The optional credential to use. credentialName takes precedence if provided.
  */
case class S3FileConnector(region: String,
                           bucket: String,
                           override val name: String,
                           override val credentialName: Option[String],
                           override val credential: Option[Credential]) extends FileConnector with AWSConnector {
  /**
    * Creates and opens an S3FileManager.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  override def getFileManager(pipelineContext: PipelineContext): FileManager = {
    val finalCredential = getCredential(pipelineContext)
    val s3 = new S3FileManager(region, bucket, finalCredential.get.awsAccessKey, finalCredential.get.awsAccessSecret,
      finalCredential.get.awsAccountId, finalCredential.get.awsRole, finalCredential.get.awsPartition)
    s3.connect()
    s3
  }
}