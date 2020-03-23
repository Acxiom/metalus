package com.acxiom.pipeline.steps

import java.net.HttpURLConnection
import java.text.SimpleDateFormat

import com.acxiom.pipeline.api.{BasicAuthorization, HttpRestClient}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aMultipart, aResponse, containing, delete, equalTo, get, post, put, urlPathEqualTo}
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
      assert(ApiSteps.exists(http, "/redirect"))
      assert(ApiSteps.getContentLength(http, "/redirect") == 500)
      assert(ApiSteps.getLastModifiedDate(http, "/redirect").getTime == date.getTime)
      assert(ApiSteps.getHeaders(http, "/redirect")("Content-Type").head == "application/json")

      wireMockServer.addStubMapping(get(urlPathEqualTo("/files/testFile"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody("this is some content")).build())
      val inputStream = ApiSteps.getInputStream(http, "/files/testFile")
      val content = Source.fromInputStream(inputStream).mkString
      inputStream.close()
      assert(content == "this is some content")
      assert(ApiSteps.getStringContent(http, "/files/testFile") == "this is some content")

      wireMockServer.addStubMapping(post(urlPathEqualTo("/files"))
        .withBasicAuth("myuser", "mypassword")
        .withRequestBody(equalTo("{body: {} }"))
        .willReturn(aResponse()
          .withBody("this is some returned content")).build())
      assert(ApiSteps.postStringContent(http, "/files", "{body: {} }", "application/json") == "this is some returned content")

      wireMockServer.addStubMapping(put(urlPathEqualTo("/files/update"))
        .withBasicAuth("myuser", "mypassword")
        .withRequestBody(equalTo("{body: {} }"))
        .willReturn(aResponse()
          .withBody("this is some returned content")).build())
      assert(ApiSteps.putStringContent(http, "/files/update", "{body: {} }") == "this is some returned content")

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFile"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NO_CONTENT)).build())
      assert(ApiSteps.delete(http, "/files/deleteFile"))

      wireMockServer.addStubMapping(delete(urlPathEqualTo("/files/deleteFileFail"))
        .withBasicAuth("myuser", "mypassword")
        .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
        .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_NOT_FOUND)).build())
      assert(!ApiSteps.delete(http, "/files/deleteFileFail"))

      val http1 = HttpRestClient("http", "localhost", wireMockServer.port())
      wireMockServer.addStubMapping(post(urlPathEqualTo("/files/testFileUpload"))
        .withBasicAuth("myuser", "mypassword")
        .withMultipartRequestBody(aMultipart()
          .withBody(containing("uploaded content")))
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)).build())
      val outputStream = ApiSteps.getOutputStream(http1, "/files/testFileUpload")
      outputStream.write("uploaded content".getBytes)
      outputStream.flush()
      outputStream.close()
    }
  }
}
