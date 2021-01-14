package com.acxiom.metalus

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.apache.commons.io.FileUtils
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import scala.io.Source

class MetadataExtractorTests extends FunSpec with BeforeAndAfterAll with Suite {
  implicit val formats: Formats = DefaultFormats

  private val HTTP_PORT = 10296
  private val HTTPS_PORT = 8446
  private val wireMockServer = new WireMockServer(HTTP_PORT, HTTPS_PORT)

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  describe("Metadata Extractor - Local Output") {
    it("Should extract executions") {
      // Create temp directories
      val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_local_test")
      tempDirectory.toFile.deleteOnExit()
      val stagingDirectory = Files.createTempDirectory("metalus_extractor_local_test")
      stagingDirectory.toFile.deleteOnExit()
      // Copy the test jars
      val tempUri = tempDirectory.toUri.toString
      val mainJar = Paths.get(URI.create(s"$tempUri/main-1.0.0.jar"))
      Files.copy(getClass.getResourceAsStream("/main-1.0.0.jar"), mainJar)
      val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
        "--excludeSteps", "true", "--excludePipelines", "true",
        "--output-path", s"${stagingDirectory.toFile.getAbsolutePath}",
        "--extractors", "com.acxiom.metalus.executions.ExecutionsMetadataExtractor")
      MetadataExtractor.main(params)
      val stagedFiles = stagingDirectory.toFile.list()
      assert(stagedFiles.length == 1)
      assert(stagedFiles.contains("executions.json"))
      // Load the file and verify that the two executions are present
      val source = Source.fromFile(new File(stagingDirectory.toFile, "executions.json"))
      val executionList = parse(source.mkString).extract[List[Map[String, Any]]]
      source.close()
      assert(executionList.size == 2)
      executionList.foreach(map => {
        assert(map.getOrElse("id", "none") == "Blank" || map.getOrElse("id", "none") == "Real")
      })
      FileUtils.deleteDirectory(tempDirectory.toFile)
      FileUtils.deleteDirectory(stagingDirectory.toFile)
    }
  }

  describe("Metadata Extractor - API Output") {
    it("Should extract executions to remote API") {
      // Create temp directories
      val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_local_test")
      tempDirectory.toFile.deleteOnExit()
      // Copy the test jars
      val tempUri = tempDirectory.toUri.toString
      val mainJar = Paths.get(URI.create(s"$tempUri/main-1.0.0.jar"))
      Files.copy(getClass.getResourceAsStream("/main-1.0.0.jar"), mainJar)
      val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
        "--excludeSteps", "true", "--excludePipelines", "true",
        "--api-url", s"http://localhost:${wireMockServer.port()}",
        "--extractors", "com.acxiom.metalus.executions.ExecutionsMetadataExtractor")
      wireMockServer.addStubMapping(get(urlEqualTo("/api/v1/executions/Blank"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/json")
          .withStatus(200)
        ).build())
      wireMockServer.addStubMapping(put(urlEqualTo("/api/v1/executions/Blank"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/json")
          .withStatus(200)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/api/v1/executions/Real"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/json")
          .withStatus(404)
        ).build())
      wireMockServer.addStubMapping(post(urlEqualTo("/api/v1/executions"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/json")
          .withStatus(201)
        ).build())

      MetadataExtractor.main(params)
      FileUtils.deleteDirectory(tempDirectory.toFile)
    }
  }
}
