package com.acxiom.metalus.aws.pipeline.connectors

import com.acxiom.metalus.Credential
import com.acxiom.metalus.connectors.{ConnectorProvider, FileConnectorType}

import java.net.URI
import scala.util.Try

final class AWSConnectorProvider extends ConnectorProvider {

  override def ordering: Int = Int.MaxValue - 200 // load before base spark providers

  override def getConnectorProviders: ProviderFunction = {
    case (name, "s3" | "s3a" | "s3n", FileConnectorType(_), Some(options)) if options.contains("region") &&
      options.contains("bucket") =>
      S3FileConnector(options("region").toString,
        Option("bucket").toString,
        name,
        options.get("credentialName").map(_.toString),
        getCredentials(options)
      )
    case (name, S3URI(uri), FileConnectorType(_), Some(options)) if options.contains("region") =>
      S3FileConnector(options("region").toString,
        Option(uri.getHost).getOrElse(Option("bucket").toString),
        name,
        options.get("credentialName").map(_.toString),
        getCredentials(options)
      )
  }

  private def getCredentials(options: Map[String, Any]): Option[Credential] =
    options.get("credential").flatMap(c => Try(c.asInstanceOf[Credential]).toOption)
}

object S3URI {
  def unapply(uri: String): Option[URI] = Try(new URI(uri)).toOption.collect {
    case u if u.getScheme.startsWith("s3") => u
  }
}
