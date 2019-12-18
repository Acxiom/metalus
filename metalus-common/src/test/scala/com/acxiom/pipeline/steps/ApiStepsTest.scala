package com.acxiom.pipeline.steps

import java.net.HttpURLConnection

import com.acxiom.pipeline.api.BasicAuthorization
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aMultipart, aResponse, containing, delete, equalTo, get, post, urlPathEqualTo}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import scala.io.Source

class ApiStepsTest extends FunSpec with BeforeAndAfterAll {
  private val HTTP_PORT = 10293

  private val wireMockServer = new WireMockServer(HTTP_PORT)

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  describe("ApiSteps - HttpRestClient") {
    it("Should validate different functions") {
      val http = ApiSteps.createHttpRestClient(wireMockServer.baseUrl(), Some(BasicAuthorization("myuser", "mypassword")))
      // Call connect and disconnect to ensure they do not change any behaviors
      wireMockServer.addStubMapping(get(urlPathEqualTo("/files"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)
          .withHeader("content-type", "application/json")
          .withHeader("content-length", "500")
        ).build())
      assert(http.exists("/files"))
      assert(http.getContentLength("/files") == 500)

      wireMockServer.addStubMapping(get(urlPathEqualTo("/files/testFile"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody("this is some content")).build())
      val inputStream = http.getInputStream("/files/testFile")
      val content = Source.fromInputStream(inputStream).mkString
      inputStream.close()
      assert(content == "this is some content")
      assert(http.getStringContent("/files/testFile") == "this is some content")

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFile"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NO_CONTENT)).build())
      assert(http.delete("/files/deleteFile"))

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFileFail"))
        .withBasicAuth("myuser", "mypassword")
        .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NOT_FOUND)).build())
      assert(!http.delete("/files/deleteFileFail"))

      val http1 = ApiSteps.createHttpRestClientFromParameters("http", "localhost", wireMockServer.port())
      wireMockServer.addStubMapping(post(urlPathEqualTo("/files/testFileUpload"))
        .withBasicAuth("myuser", "mypassword")
        .withMultipartRequestBody(aMultipart()
          .withBody(containing("uploaded content")))
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)).build())
      val outputStream = http1.getOutputStream("/files/testFileUpload")
      outputStream.write("uploaded content".getBytes)
      outputStream.flush()
      outputStream.close()
    }
  }
}
