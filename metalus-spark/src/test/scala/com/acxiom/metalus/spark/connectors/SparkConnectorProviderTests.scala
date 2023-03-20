package com.acxiom.metalus.spark.connectors

import com.acxiom.metalus.UserNameCredential
import com.acxiom.metalus.connectors.ConnectorProvider
import org.scalatest.funspec.AnyFunSpec

class SparkConnectorProviderTests extends AnyFunSpec {

  describe("SparkConnectorProvider - Basic") {
    it("should build a spark jdbc connector") {
      val parameters = Map("credential" -> UserNameCredential(Map("username" -> "chicken", "password" -> "mealworms")))
      val res = ConnectorProvider.getConnector("test", "spark:jdbc:derby:memory:test", Some("DATA"),
        Some(parameters))
      assert(res.exists(_.isInstanceOf[JDBCSparkDataConnector]))
      val jdbc = res.get.asInstanceOf[JDBCSparkDataConnector]
      assert(jdbc.url == "jdbc:derby:memory:test")
      assert(jdbc.name == "test")
      assert(jdbc.credential.exists(_.isInstanceOf[UserNameCredential]))
    }

    it("should build a default spark connector") {
      val res = ConnectorProvider.getConnector("test", "spark:s3://kfc/chickens.parquet", Some("DATA"), None)
      assert(res.exists(_.isInstanceOf[DefaultSparkDataConnector]))
      val spark = res.get.asInstanceOf[DefaultSparkDataConnector]
      assert(spark.name == "test")
    }
  }

}
