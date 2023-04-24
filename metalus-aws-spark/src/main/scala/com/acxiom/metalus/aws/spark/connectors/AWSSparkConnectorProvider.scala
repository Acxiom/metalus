package com.acxiom.metalus.aws.spark.connectors

import com.acxiom.metalus.Credential
import com.acxiom.metalus.connectors.{ConnectorProvider, DataConnectorType}
import com.acxiom.metalus.spark.connectors.SparkURI

import scala.util.Try

final class AWSSparkConnectorProvider extends ConnectorProvider {

  override def ordering: Int = Int.MaxValue - 110 // load before base spark providers

  override def getConnectorProviders: ProviderFunction = {
    case (name, SparkURI(uri), DataConnectorType(_), options) if uri.getScheme == "kinesis" =>
      KinesisSparkDataConnector(
        uri.getHost,
        options.flatMap(_.get("region")).map(_.toString).getOrElse("us-east-1"),
        options.flatMap(_.get("partitionKey")).map(_.toString),
        options.flatMap(_.get("partitionKeyIndex")).flatMap(i => Try(i.toString.toInt).toOption),
        name = name,
        credentialName = options.flatMap(_.get("credentialName")).map(_.toString),
        credential = getCredentials(options)
      )
    case (name, SparkURI(uri), DataConnectorType(_), Some(options)) if uri.toString == "kinesis" && options.contains("streamName") =>
      KinesisSparkDataConnector(
        options("streamName").toString,
        options.get("region").map(_.toString).getOrElse("us-east-1"),
        options.get("partitionKey").map(_.toString),
        options.get("partitionKeyIndex").flatMap(i => Try(i.toString.toInt).toOption),
        name = name,
        credentialName = options.get("credentialName").map(_.toString),
        credential = getCredentials(Some(options))
      )
    case (name, "kinesis", DataConnectorType(_), Some(options)) if options.contains("streamName") =>
      KinesisSparkDataConnector(
        options("streamName").toString,
        options.get("region").map(_.toString).getOrElse("us-east-1"),
        options.get("partitionKey").map(_.toString),
        options.get("partitionKeyIndex").flatMap(i => Try(i.toString.toInt).toOption),
        name = name,
        credentialName = options.get("credentialName").map(_.toString),
        credential = getCredentials(Some(options))
      )
  }

  private def getCredentials(options: Option[Map[String, Any]]): Option[Credential] =
    options.flatMap(_.get("credential")).flatMap(c => Try(c.asInstanceOf[Credential]).toOption)
}
