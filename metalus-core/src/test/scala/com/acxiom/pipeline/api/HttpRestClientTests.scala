package com.acxiom.pipeline.api

import com.acxiom.pipeline.Constants
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.net.HttpURLConnection
import java.text.SimpleDateFormat
import scala.io.Source

class HttpRestClientTests extends AnyFunSpec with BeforeAndAfterAll with Suite {

  private val HTTP_PORT = 10293
  private val HTTPS_PORT = 8443

  private val wireMockServer = new WireMockServer(HTTP_PORT, HTTPS_PORT)

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  describe("HttpRestClients") {
    it("Should fail for protocols other than http and https") {
      val thrown = intercept[IllegalArgumentException] {
        HttpRestClient("file://this.should.fail")
      }
      assert(thrown.getMessage == "Only http and https protocols are supported!")
    }

    it("Should validate different functions") {
      val authorization = BasicAuthorization("myuser", "mypassword")
      val http = HttpRestClient(s"https://localhost:$HTTPS_PORT", authorization, allowSelfSignedCertificates = true)
      val dateFormat = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss zzz")
      val dateString = "Mon, 23 Mar 2020 07:26:45 GMT"
      val date = dateFormat.parse(dateString)
      // Call connect and disconnect to ensure they do not change any behaviors
      wireMockServer.addStubMapping(get(urlPathEqualTo("/redirect"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withHeader("Location", "/redirect1/files")
          .withStatus(HttpURLConnection.HTTP_MOVED_PERM)
        ).build())
      wireMockServer.addStubMapping(get(urlPathEqualTo("/redirect1/files"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)
          .withHeader("content-type", "application/json")
          .withHeader("content-length", "500")
          .withHeader("last-modified", dateString)
        ).build())
      assert(http.exists("/redirect"))
      assert(http.getContentLength("/redirect") == 500)
      assert(http.getLastModifiedDate("/redirect").getTime == date.getTime)
      assert(http.getHeaders("/redirect")("Content-Type").head == "application/json")

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

      wireMockServer.addStubMapping(post(urlPathEqualTo("/files"))
        .withBasicAuth("myuser", "mypassword")
        .withRequestBody(equalTo("{body: {} }"))
        .willReturn(aResponse()
          .withBody("this is some returned content")).build())
      assert(http.postJsonContent("/files", "{body: {} }") == "this is some returned content")

      wireMockServer.addStubMapping(put(urlPathEqualTo("/files/update"))
        .withBasicAuth("myuser", "mypassword")
        .withRequestBody(equalTo("{body: {} }"))
        .willReturn(aResponse()
          .withBody("this is some returned content")).build())
      assert(http.putJsonContent("/files/update", "{body: {} }") == "this is some returned content")

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFile"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NO_CONTENT)).build())
      assert(http.delete("/files/deleteFile"))

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFileFail"))
        .withBasicAuth("myuser", "mypassword")
        .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NOT_FOUND)).build())
      assert(!http.delete("/files/deleteFileFail"))

      val http1 = HttpRestClient("http", "localhost", wireMockServer.port())
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

      val http2 = HttpRestClient(s"http://localhost:$HTTP_PORT", authorization)
      wireMockServer.addStubMapping(post(urlPathEqualTo("/files/unsecured"))
        .withBasicAuth("myuser", "mypassword")
        .withRequestBody(equalTo("{body: {} }"))
        .willReturn(aResponse()
          .withBody("this is some unsecured content")).build())
      assert(http2.postStringContent("/files/unsecured", "{body: {} }", Constants.JSON_CONTENT_TYPE)
        == "this is some unsecured content")

      val http3 = HttpRestClient("http", "localhost", HTTP_PORT, authorization)
      wireMockServer.addStubMapping(put(urlPathEqualTo("/files/unsecured/update"))
        .withBasicAuth("myuser", "mypassword")
        .withRequestBody(equalTo("{body: {} }"))
        .willReturn(aResponse()
          .withBody("this is some updated unsecured content")).build())
      assert(http3.putStringContent("/files/unsecured/update", "{body: {} }", Constants.JSON_CONTENT_TYPE)
        == "this is some updated unsecured content")
    }
  }
}
