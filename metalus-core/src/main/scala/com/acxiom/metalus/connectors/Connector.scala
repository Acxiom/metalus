package com.acxiom.metalus.connectors

import com.acxiom.metalus.sql.Row
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.{Constants, Credential, PipelineContext}

trait Connector {
  def name: String

  def credentialName: Option[String]

  def credential: Option[Credential]

  def connectorType: String

  /**
   * Returns a DataRowReader or None. The reader can be used to window data from the connector.
   *
   * @param properties Optional properties required by the reader.
   * @param pipelineContext The current PipelineContext
   * @return Returns a DataRowReader or None.
   */
  def getReader(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowReader] = None

  /**
   * Returns a DataRowWriter or None. The writer can be used to window data to the connector.
   *
   * @param properties Optional properties required by the writer.
   * @param pipelineContext The current PipelineContext
   * @return Returns a DataRowWriter or None.
   */
  def getWriter(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowWriter] = None

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
