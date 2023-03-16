package com.acxiom.metalus.connectors

import com.acxiom.metalus.{Credential, PipelineContext}

trait Connector {
  def name: String

  def credentialName: Option[String]

  def credential: Option[Credential]

  def connectorType: String

  /**
    * Using the provided PipelineContext and the optional credentialName and credential, this function will
    * attempt to provide a Credential for use by the connector.
    *
    * @param pipelineContext The current PipelineContext for this session.
    * @return A credential or None.
    */
  protected def getCredential(pipelineContext: PipelineContext): Option[Credential] = {
    if (credentialName.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName.get)
    } else {
      credential
    }
  }
}
