package com.acxiom.metalus.connectors

import com.acxiom.metalus.fs.{FileManager, SFTPFileManager}
import com.acxiom.metalus.{Credential, PipelineContext, UserNameCredential}

/**
  * Provides an implementation of FileConnector that works with SFTP.
  *
  * @param hostName       The host name of the SFTP resource
  * @param name           The name of this connector
  * @param credentialName The optional name of the credential to provide the CredentialProvider
  * @param credential     The optional credential to use. credentialName takes precedence if provided.
  * @param port           The optional SFTP port
  * @param knownHosts     The optional file path to the known_hosts file
  * @param bulkRequests   The optional number of requests that may be sent at one time
  * @param config         Optional config options
  * @param timeout        Optional connection timeout
  */
case class SFTPFileConnector(hostName: String,
                             override val name: String,
                             override val credentialName: Option[String],
                             override val credential: Option[Credential],
                             port: Option[Int] = None,
                             knownHosts: Option[String] = None,
                             bulkRequests: Option[Int] = None,
                             config: Option[Map[String, String]] = None,
                             timeout: Option[Int] = None) extends FileConnector {
  /**
    * Creates and opens a FileManager.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A FileManager for this specific connector type
    */
  override def getFileManager(pipelineContext: PipelineContext): FileManager = {
    val finalCredential = getCredential(pipelineContext).asInstanceOf[Option[UserNameCredential]]
    val creds = if (finalCredential.isDefined) {
      (Some(finalCredential.get.name), Some(finalCredential.get.password))
    } else {
      (None, None)
    }
    val fm = SFTPFileManager(hostName, port, creds._1, creds._2, knownHosts, bulkRequests, config, timeout)
    fm.connect()
    fm
  }
}
