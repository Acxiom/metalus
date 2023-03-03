package com.acxiom.metalus

import com.acxiom.metalus.fs.FileManager
import com.acxiom.metalus.parser.JsonParser
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import scala.io.Source

class MetadataExtractorTests extends AnyFunSpec with BeforeAndAfterAll {
  private val HTTP_PORT = 10296
  private val HTTPS_PORT = 8446
  private val wireMockServer = new WireMockServer(HTTP_PORT, HTTPS_PORT)

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }
// TODO Change to Pipelines
  describe("Metadata Extractor") {
    describe("Local Output") {
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
          "--excludeSteps", "true", "--output-path", s"${stagingDirectory.toFile.getAbsolutePath}")
        MetadataExtractor.main(params)
        val stagedFiles = stagingDirectory.toFile.list()
        assert(stagedFiles.length == 1)
        assert(stagedFiles.contains("pipelines.json"))
        // Load the file and verify that the two pipelines are present
        val source = Source.fromFile(new File(stagingDirectory.toFile, "pipelines.json"))
        val pipelineList = JsonParser.parsePipelineJson(source.mkString)
        source.close()
        assert(pipelineList.isDefined)
        assert(pipelineList.get.size == 2)
        assert(pipelineList.get.exists(_.id.getOrElse("") == "blank"))
        assert(pipelineList.get.exists(_.id.getOrElse("") == "real"))
        FileManager.deleteNio(tempDirectory.toFile)
        FileManager.deleteNio(stagingDirectory.toFile)
      }
    }

    describe("API Output") {
      it("Should extract executions to remote API") {
        // Create temp directories
        val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_local_test")
        tempDirectory.toFile.deleteOnExit()
        // Copy the test jars
        val tempUri = tempDirectory.toUri.toString
        val mainJar = Paths.get(URI.create(s"$tempUri/main-1.0.0.jar"))
        Files.copy(getClass.getResourceAsStream("/main-1.0.0.jar"), mainJar)
        val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
          "--excludeSteps", "true", "--api-url", s"http://localhost:${wireMockServer.port()}")
        wireMockServer.addStubMapping(get(urlEqualTo("/api/v1/pipelines/blank"))
          .willReturn(aResponse()
            .withHeader("content-type", "application/json")
            .withStatus(200)
          ).build())
        wireMockServer.addStubMapping(put(urlEqualTo("/api/v1/pipelines/blank"))
          .willReturn(aResponse()
            .withHeader("content-type", "application/json")
            .withStatus(200)
          ).build())
        wireMockServer.addStubMapping(get(urlEqualTo("/api/v1/pipelines/real"))
          .willReturn(aResponse()
            .withHeader("content-type", "application/json")
            .withStatus(404)
          ).build())
        wireMockServer.addStubMapping(post(urlEqualTo("/api/v1/pipelines"))
          .willReturn(aResponse()
            .withHeader("content-type", "application/json")
            .withStatus(201)
          ).build())

        MetadataExtractor.main(params)
        FileManager.deleteNio(tempDirectory.toFile)
      }
    }
  }
}
