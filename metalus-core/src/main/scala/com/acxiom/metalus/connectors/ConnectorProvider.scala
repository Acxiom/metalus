package com.acxiom.metalus.connectors

import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.connectors.jdbc.JDBCDataConnector

import java.net.URI
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.Try

object ConnectorProvider {

  private lazy val providers =
    ServiceLoader.load(classOf[ConnectorProvider]).asScala
      .toList
      .sortBy(_.ordering)
      .map(_.getConnectorProviders)
      .reduceLeft(_ orElse _).lift


  // TODO Lookup connector by name from some sort of connector cache
  def getConnector(name: String,
                   uri: String,
                   connectorType: Option[String],
                   parameters: Option[Map[String, Any]]): Option[Connector] =
    providers(name, uri, connectorType, parameters)

}


trait ConnectorProvider {

  type ProviderFunction = PartialFunction[(String, String, Option[String], Option[Map[String, Any]]), Connector]

  def getConnectorProviders: ProviderFunction

  def ordering: Int = 0

}

final class DefaultConnectorProvider extends ConnectorProvider {

  override def ordering: Int = Int.MaxValue // ensure this is always the last set of functions evaluated.

  override def getConnectorProviders: ProviderFunction = {
    case (name, JDBCURI(uri), DataConnectorType(_), options) =>
      val filteredOptions = options.map(m => m - "credentialName" - "credential")
      val credentialName = options.flatMap(_.get("credentialName").map(_.toString))
      val credential = getCredentials(options)
      JDBCDataConnector(uri.toString, name, credentialName, credential, filteredOptions)
    case (name, SFTPURI(uri), FileConnectorType(_), options) =>
      val credentialName = options.flatMap(_.get("credentialName").map(_.toString))
      val knownHosts = options.flatMap(_.get("knownHosts").map(_.toString))
      val bulkRequests = options.flatMap(_.get("bulkRequests").flatMap(i => Try(i.toString.toInt).toOption))
      val timeout = options.flatMap(_.get("timeout").flatMap(i => Try(i.toString.toInt).toOption))
      val config: Option[Map[String, String]] = options.map(_.flatMap {
        case ("credentialName" | "credential" | "timeout" | "bulkRequests" | "knownHosts", _) => None
        case (k, v) => Some(k -> v.toString)
      })
      SFTPFileConnector.fromURI(uri, name, credentialName, getCredentials(options), knownHosts, bulkRequests, config,
        timeout)
    case (name, _, FileConnectorType(_), options) =>
      val credentialName = options.flatMap(_.get("credentialName").map(_.toString))
      val credential = getCredentials(options)
      LocalFileConnector(name, credentialName, credential)
    case (name, _, InMemoryDataConnectorType(_), options) =>
      val credentialName = options.flatMap(_.get("credentialName").map(_.toString))
      val credential = getCredentials(options)
      InMemoryDataConnector(name, credentialName, credential)
  }

  private def getCredentials(options: Option[Map[String, Any]]): Option[Credential] =
    options.flatMap(_.get("credential")).flatMap(c => Try(c.asInstanceOf[Credential]).toOption)

}

object JDBCURI {
  def unapply(uri: String): Option[URI] = Try(new URI(uri)).toOption.filter(_.getScheme == "jdbc")
}

object SFTPURI {
  def unapply(uri: String): Option[URI] = Try(new URI(uri)).toOption.filter(_.getScheme == "sftp")
}

object FileConnectorType {
  def unapply(connectorType: Option[String]): Option[String] =  connectorType.map(_.toUpperCase) match {
    case Some("FILE") | None => Some("FILE")
    case _ => None
  }
}

object DataConnectorType {
  def unapply(connectorType: Option[String]): Option[String] = connectorType.map(_.toUpperCase) match {
    case Some("DATA") | None => Some("DATA")
    case _ => None
  }
}

