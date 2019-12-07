package com.acxiom.pipeline

import java.net.HttpURLConnection

import com.acxiom.pipeline.fs.HttpRestClient
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aMultipart, aResponse, containing, delete, equalTo, get, post, urlPathEqualTo}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.io.Source

class HttpRestClientTests  extends FunSpec with BeforeAndAfterAll with Suite {

  private val HTTP_PORT = 10293

  private val wireMockServer = new WireMockServer(HTTP_PORT)

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  describe("FileManager - Http") {
    it("Should fail for protocols other than http and https") {
      val thrown = intercept[IllegalArgumentException] {
        HttpRestClient("file://this.should.fail")
      }
      assert(thrown.getMessage == "Only http and https protocols are supported!")
    }

    it("Should validate different functions") {
      val http = HttpRestClient(wireMockServer.baseUrl())
      // Call connect and disconnect to ensure they do not change any behaviors
      wireMockServer.addStubMapping(get(urlPathEqualTo("/files"))
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)
          .withHeader("content-type", "application/json")
          .withHeader("content-length", "500")
        ).build())
      assert(http.exists("/files"))
      assert(http.getContentLength("/files") == 500)

      wireMockServer.addStubMapping(get(urlPathEqualTo("/files/testFile"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody("this is some content")).build())
      val inputStream = http.getInputStream("/files/testFile")
      val content = Source.fromInputStream(inputStream).mkString
      inputStream.close()
      assert(content == "this is some content")
      assert(http.getStringContent("/files/testFile") == "this is some content")

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFile"))
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NO_CONTENT)).build())
      assert(http.delete("/files/deleteFile"))

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFileFail"))
        .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NOT_FOUND)).build())
      assert(!http.delete("/files/deleteFileFail"))

      val http1 = HttpRestClient("http", "localhost", wireMockServer.port())
      wireMockServer.addStubMapping(post(urlPathEqualTo("/files/testFileUpload"))
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
