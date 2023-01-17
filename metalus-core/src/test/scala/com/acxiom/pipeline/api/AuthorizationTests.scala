package com.acxiom.pipeline.api

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, post, urlPathEqualTo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.net.URL

class AuthorizationTests extends AnyFunSpec with BeforeAndAfterAll {
  private val HTTP_PORT = 10295

  private val wireMockServer = new WireMockServer(HTTP_PORT)

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  describe("Should test SessionAuthorization") {
    it("Should verify the SessionAuthorization") {
      wireMockServer.addStubMapping(post(urlPathEqualTo("/api/v1/users/login"))
        .willReturn(aResponse()
          .withHeader("Set-Cookie", "somevalue=session_id")).build())

      val sessionAuth = SessionAuthorization("test", "test", s"${wireMockServer.baseUrl()}/api/v1/users/login")
      val connection = new URL(wireMockServer.baseUrl()).openConnection()
      sessionAuth.authorize(connection)
      assert(connection.getRequestProperty("Cookie") == "somevalue=session_id")
    }
  }
}
