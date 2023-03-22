package com.acxiom.metalus.aws.spark.connectors

import com.acxiom.metalus.connectors.ConnectorProvider
import com.acxiom.metalus.aws.utils.AWSBasicCredential
import org.scalatest.funspec.AnyFunSpec

class AWSSparkConnectorProviderTests extends AnyFunSpec {

  describe("AWSSparkConnectorProvider - Basic") {
    it("should build a kinesis data connector") {
      val cred = new AWSBasicCredential(Map("accessKeyId" -> "chicken", "secretAccessKey" -> "mealworms"))
      val parameters = Map(
        "credential" -> cred,
        "partitionKey" -> "breed"
      )
      val res = ConnectorProvider.getConnector("test", "spark:kinesis://chickens", Some("DATA"),
        Some(parameters))
      assert(res.exists(_.isInstanceOf[KinesisSparkDataConnector]))
      val kinesis = res.get.asInstanceOf[KinesisSparkDataConnector]
      assert(kinesis.partitionKey.contains("breed"))
      assert(kinesis.name == "test")
      assert(kinesis.credential.exists(_.isInstanceOf[AWSBasicCredential]))
      assert(kinesis.streamName == "chickens")
    }

    it("should build a kinesis data connector if uri equals kinesis") {
      val cred = new AWSBasicCredential(Map("accessKeyId" -> "chicken", "secretAccessKey" -> "mealworms"))
      val parameters = Map(
        "credential" -> cred,
        "partitionKey" -> "breed",
        "streamName" -> "chickens"
      )
      val res = ConnectorProvider.getConnector("test", "kinesis", Some("DATA"),
        Some(parameters))
      assert(res.exists(_.isInstanceOf[KinesisSparkDataConnector]))
      val kinesis = res.get.asInstanceOf[KinesisSparkDataConnector]
      assert(kinesis.partitionKey.contains("breed"))
      assert(kinesis.name == "test")
      assert(kinesis.credential.exists(_.isInstanceOf[AWSBasicCredential]))
      assert(kinesis.streamName == "chickens")
    }
  }

}
