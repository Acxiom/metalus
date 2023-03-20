package com.acxiom.metalus.connectors

import com.acxiom.metalus.UserNameCredential
import com.acxiom.metalus.connectors.jdbc.JDBCDataConnector
import org.scalatest.funspec.AnyFunSpec

class ConnectorProviderTests extends AnyFunSpec {

  describe("ConnectorProvider - Basic") {
    it("should build a jdbc data connector") {
      val parameters = Map("credential" -> UserNameCredential(Map("username" -> "chicken", "password" -> "mealworms")))
      val res = ConnectorProvider.getConnector("test", "jdbc:derby:memory:test", Some("DATA"),
        Some(parameters))
      assert(res.exists(_.isInstanceOf[JDBCDataConnector]))
      val jdbc = res.get.asInstanceOf[JDBCDataConnector]
      assert(jdbc.url == "jdbc:derby:memory:test")
      assert(jdbc.name == "test")
      assert(jdbc.credential.exists(_.isInstanceOf[UserNameCredential]))
    }

    it("should build an sftp file connector") {
      val parameters = Map(
        "credential" -> UserNameCredential(Map("username" -> "chicken", "password" -> "mealworms")),
        "timeout" -> "5000",
        "chickens" -> "rule"
      )
      val res = ConnectorProvider.getConnector("test", "sftp://kfc:8080/", None,
        Some(parameters))
      assert(res.exists(_.isInstanceOf[SFTPFileConnector]))
      val sftp = res.get.asInstanceOf[SFTPFileConnector]
      assert(sftp.name == "test")
      assert(sftp.credential.isDefined)
      assert(sftp.port.contains("8080".toInt))
      assert(sftp.timeout.contains("5000".toInt))
      assert(sftp.config.exists(_.get("chickens").contains("rule")))
    }

    it("should build a local file connector") {
      val res = ConnectorProvider.getConnector("test", "chickens.txt", Some("FILE"), None)
      assert(res.exists(_.isInstanceOf[LocalFileConnector]))
    }

    it("should return NONE if it cannot match to a connector") {
      val res = ConnectorProvider.getConnector("test", "chickens.txt", Some("DATA"), None)
      assert(res.isEmpty)
    }
  }

}
