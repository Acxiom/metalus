package com.acxiom.metalus.spark.connectors

import com.acxiom.metalus.Credential
import com.acxiom.metalus.connectors.{ConnectorProvider, DataConnectorType}

import java.net.URI
import scala.util.Try

final class SparkConnectorProvider extends ConnectorProvider {

  override def ordering: Int = Int.MaxValue - 100 // load before base providers

  override def getConnectorProviders: ProviderFunction = {
    case (name, SparkURI(uri), DataConnectorType(_), options) if uri.getScheme == "jdbc" =>
      val credentialName = options.flatMap(_.get("credentialName").map(_.toString))
      val credential = getCredentials(options)
      val predicates = options.flatMap(_.get("predicates").flatMap(l => Try(l.asInstanceOf[List[String]]).toOption))
      JDBCSparkDataConnector(uri.toString, predicates, name, credentialName, credential)
    case (name, SparkURI(_), DataConnectorType(_), options) =>
      val credentialName = options.flatMap(_.get("credentialName").map(_.toString))
      val credential = getCredentials(options)
      DefaultSparkDataConnector(name, credentialName, credential)
  }

  private def getCredentials(options: Option[Map[String, Any]]): Option[Credential] =
    options.flatMap(_.get("credential")).flatMap(c => Try(c.asInstanceOf[Credential]).toOption)

}

object SparkURI {
  def unapply(uri: String): Option[URI] = {
    Try(new URI(uri)).toOption.collect{
      case u if u.getScheme == "spark" => new URI(uri.substring(uri.indexOf(':') + 1))
      case u if uri.startsWith("spark") => u
    }
  }
}
