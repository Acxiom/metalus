package com.acxiom.metalus.aws.pipeline.connectors

import com.acxiom.metalus.connectors.ConnectorProvider
import com.acxiom.metalus.aws.utils.AWSBasicCredential
import org.scalatest.funspec.AnyFunSpec

class AWSConnectorProviderTests extends AnyFunSpec {

  describe("AWSConnectorProvider - Basic") {
    it("should build a s3 file connector") {
      val cred = new AWSBasicCredential(Map("accessKeyId" -> "chicken", "secretAccessKey" -> "mealworms"))
      val parameters = Map(
        "credential" -> cred,
        "region" -> "us-east-1"
      )
      val res = ConnectorProvider.getConnector("test", "s3://kfc/", Some("FILE"),
        Some(parameters))
      assert(res.exists(_.isInstanceOf[S3FileConnector]))
      val s3 = res.get.asInstanceOf[S3FileConnector]
      assert(s3.region.contains("us-east-1"))
      assert(s3.name == "test")
      assert(s3.credential.exists(_.isInstanceOf[AWSBasicCredential]))
      assert(s3.bucket == "kfc")
    }

    it("should build a s3 file connector if uri equals s3") {
      val cred = new AWSBasicCredential(Map("accessKeyId" -> "chicken", "secretAccessKey" -> "mealworms"))
      val parameters = Map(
        "credential" -> cred,
        "region" -> "us-east-1",
        "bucket" -> "kfc"
      )
      val res = ConnectorProvider.getConnector("test", "s3a", None,
        Some(parameters))
      assert(res.exists(_.isInstanceOf[S3FileConnector]))
      val s3 = res.get.asInstanceOf[S3FileConnector]
      assert(s3.region.contains("us-east-1"))
      assert(s3.name == "test")
      assert(s3.credential.exists(_.isInstanceOf[AWSBasicCredential]))
      assert(s3.bucket == "kfc")
    }
  }

}
