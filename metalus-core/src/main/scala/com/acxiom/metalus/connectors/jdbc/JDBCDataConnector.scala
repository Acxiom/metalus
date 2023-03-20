package com.acxiom.metalus.connectors.jdbc

import com.acxiom.metalus.{Credential, PipelineContext, PipelineException, UserNameCredential}
import com.acxiom.metalus.connectors.DataConnector
import com.acxiom.metalus.sql.DataReferenceOrigin
import com.acxiom.metalus.sql.jdbc.{BasicJDBCDataReference, JDBCDataReference}

case class JDBCDataConnector(url: String,
                             name: String,
                             credentialName: Option[String] = None,
                             credential: Option[Credential] = None,
                             defaultProperties: Option[Map[String, Any]] = None) extends DataConnector {
  override def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): JDBCDataReference[_] = {
    val dbtable = {
      pipelineContext.contextManager.getContext("")
      val tmp = properties.flatMap(_.get("dbtable")).map(_.toString)
      if (tmp.isEmpty) {
        throw PipelineException(message = Some("dbtable must be provided to create a JDBCDataReference"),
          pipelineProgress = pipelineContext.currentStateInfo)
      }
      tmp.get
    }
    getTable(dbtable, properties.map(_.mapValues(_.toString).filterKeys(_ != "dbtable").toMap[String, String]), pipelineContext)
  }

  def getTable(dbtable: String, properties: Option[Map[String, String]], pipelineContext: PipelineContext): JDBCDataReference[_] = {
    val info = defaultProperties.getOrElse(Map()).mapValues(_.toString).toMap[String, String] ++
      properties.getOrElse(Map()) ++
      getCredential(pipelineContext).collect {
        case unc: UserNameCredential => Map("user" -> unc.name, "password" -> unc.password)
      }.getOrElse(Map())
    BasicJDBCDataReference(() => dbtable, url, info, DataReferenceOrigin(this, Some(info)), pipelineContext)
  }
}
