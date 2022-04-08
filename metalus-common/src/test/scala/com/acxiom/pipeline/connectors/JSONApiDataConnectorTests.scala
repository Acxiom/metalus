package com.acxiom.pipeline.connectors

import com.acxiom.pipeline._
import com.acxiom.pipeline.applications.ApplicationUtils.parseValue
import com.acxiom.pipeline.utils.DriverUtils
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, requestMatching, urlPathEqualTo}
import com.github.tomakehurst.wiremock.extension.Parameters
import com.github.tomakehurst.wiremock.http.Request
import com.github.tomakehurst.wiremock.matching.{MatchResult, RequestMatcherExtension}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import java.io.File
import java.nio.file.{Files, Path}

class JSONApiDataConnectorTests extends FunSpec with BeforeAndAfterAll {
  val MASTER = "local[2]"
  val APPNAME = "json-api-connector-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  val file = new File(sparkLocalDir.toFile.getAbsolutePath, "cluster")
  private val HTTP_PORT = 10298
  private val wireMockServer = new WireMockServer(HTTP_PORT)

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    wireMockServer.start()

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    // Create the session
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val parameters = Map[String, Any](
      "credential-classes" -> "com.acxiom.pipeline.UserNameCredential",
      "username" -> "myuser",
      "password" -> "mypassword",
      "credential-parsers" -> ",,,com.acxiom.pipeline.DefaultCredentialParser")
    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")),
      credentialProvider = Some(new DefaultCredentialProvider(parameters)))
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()
    wireMockServer.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("JSONApiConnector Tests - Basic Reading/Writing") {
    it("Should read data from a list of row arrays") {
      val jsonString =
        """
          |{
          | "dataRows": [
          |   ["one", 2, "three"],
          |   ["four", 5, "six"],
          |   ["seven", 8, "nine"]
          | ]
          |}""".stripMargin
      wireMockServer.addStubMapping(get(urlPathEqualTo("/array-data"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withBody(jsonString)).build())
      val connectorJSON =
        s"""
           |{
           |  "connector": {
           |    "className": "com.acxiom.pipeline.connectors.JSONApiDataConnector",
           |    "object": {
           |      "apiHandler": {
           |        "jsonDocumentPath": "dataRows",
           |        "useRowArray": true,
           |        "hostUrl": "${wireMockServer.baseUrl()}",
           |        "authorizationClass": "com.acxiom.pipeline.api.BasicAuthorization",
           |        "allowSelfSignedCertificates": true,
           |        "schema": {
           |          "attributes": [
           |            {
           |              "name": "column1",
           |              "dataType": {
           |                "baseType": "string"
           |              }
           |            },
           |            {
           |              "name": "column2",
           |              "dataType": {
           |                "baseType": "integer"
           |              }
           |            },
           |            {
           |              "name": "column3",
           |              "dataType": {
           |                "baseType": "string"
           |              }
           |            }
           |          ]
           |        }
           |      },
           |      "name": "testApiConnector",
           |      "credentialName": "myuser"
           |    }
           |  }
           |}
           |""".stripMargin
      implicit val f: Formats = DefaultFormats
      val globals = JsonMethods.parse(connectorJSON).extractOpt[Map[String, Any]]
      val parsedGlobals = globals.map { baseGlobals =>
        baseGlobals.foldLeft(Map[String, Any]())((rootMap, entry) => parseValue(rootMap, entry._1, entry._2, Some(pipelineContext)))
      }
      val connector = parsedGlobals.get("connector").asInstanceOf[JSONApiDataConnector]
      // Verify load
      val df = connector.load(Some("array-data"), pipelineContext)
      assert(df.count() == 3)
      val rows = df.collectAsList()
      assert(rows.get(0).getString(0) == "one")
      assert(rows.get(0).getInt(1) == 2)
      assert(rows.get(0).getString(2) == "three")
      assert(rows.get(1).getString(0) == "four")
      assert(rows.get(1).getInt(1) == 5)
      assert(rows.get(1).getString(2) == "six")
      assert(rows.get(2).getString(0) == "seven")
      assert(rows.get(2).getInt(1) == 8)
      assert(rows.get(2).getString(2) == "nine")

      // Verify write
      wireMockServer.addStubMapping(requestMatching(new RequestMatcherExtension() {
        override def `match`(request: Request, parameters: Parameters): MatchResult = {
          implicit val f: Formats = DefaultFormats
          val bodyMap = JsonMethods.parse(request.getBodyAsString).extract[Map[String, Any]]
          MatchResult.of(
            request.getUrl == "/array-post" &&
              bodyMap.nonEmpty &&
              bodyMap.contains("dataRows") &&
              (bodyMap("dataRows").asInstanceOf[List[List[Any]]].head == List("one", 2, "three") ||
                bodyMap("dataRows").asInstanceOf[List[List[Any]]].head == List("four", 5, "six") ||
                bodyMap("dataRows").asInstanceOf[List[List[Any]]].head == List("seven", 8, "nine")))
        }
      })
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withBody(jsonString)).build())
      connector.write(df, Some("/array-post"), pipelineContext)
    }

    it("Should read data from a list of row objects") {
      val jsonString =
        """
          |{
          | "dataRows": [
          |   {"column1": "one", "column2": 2, "column3": "three"},
          |   {"column1": "four", "column2": 5, "column3": "six"},
          |   {"column1": "seven", "column2": 8, "column3": "nine"}
          | ]
          |}""".stripMargin
      wireMockServer.addStubMapping(get(urlPathEqualTo("/map-data"))
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withBody(jsonString)).build())
      val handlerJson =
        s"""
           |{
           |        "jsonDocumentPath": "dataRows",
           |        "useRowArray": false,
           |        "hostUrl": "${wireMockServer.baseUrl()}",
           |        "authorizationClass": "com.acxiom.pipeline.api.BasicAuthorization",
           |        "allowSelfSignedCertificates": true,
           |        "schema": {
           |          "attributes": [
           |            {
           |              "name": "column1",
           |              "dataType": {
           |                "baseType": "string"
           |              }
           |            },
           |            {
           |              "name": "column2",
           |              "dataType": {
           |                "baseType": "integer"
           |              }
           |            },
           |            {
           |              "name": "column3",
           |              "dataType": {
           |                "baseType": "string"
           |              }
           |            }
           |          ]
           |        }
           |      }""".stripMargin
      implicit val f: Formats = DefaultFormats
      val handler = DriverUtils.parseJson(handlerJson, "com.acxiom.pipeline.connectors.ApiHandler").asInstanceOf[ApiHandler]
      val credential = UserNameCredential(Map("username" -> "myuser", "password" -> "mypassword"))
      val connector = JSONApiDataConnector(handler, "testApiConnector", None, Some(credential))
      val df = connector.load(Some("map-data"), pipelineContext)
      assert(df.count() == 3)
      val rows = df.collectAsList()
      assert(rows.get(0).getString(0) == "one")
      assert(rows.get(0).getInt(1) == 2)
      assert(rows.get(0).getString(2) == "three")
      assert(rows.get(1).getString(0) == "four")
      assert(rows.get(1).getInt(1) == 5)
      assert(rows.get(1).getString(2) == "six")
      assert(rows.get(2).getString(0) == "seven")
      assert(rows.get(2).getInt(1) == 8)
      assert(rows.get(2).getString(2) == "nine")

      // Verify write
      wireMockServer.addStubMapping(requestMatching(new RequestMatcherExtension() {
        override def `match`(request: Request, parameters: Parameters): MatchResult = {
          implicit val f: Formats = DefaultFormats
          val bodyMap = JsonMethods.parse(request.getBodyAsString).extract[Map[String, Any]]
          MatchResult.of(
            request.getUrl == "/array-post" &&
              bodyMap.nonEmpty &&
              bodyMap.contains("dataRows") &&
              (bodyMap("dataRows").asInstanceOf[List[Map[String, Any]]].head("column1") == "one" ||
                bodyMap("dataRows").asInstanceOf[List[Map[String, Any]]].head("column1") == "four" ||
                bodyMap("dataRows").asInstanceOf[List[Map[String, Any]]].head("column1") == "seven"))
        }
      })
        .withBasicAuth("myuser", "mypassword")
        .willReturn(aResponse()
          .withBody(jsonString)).build())
      connector.write(df, Some("/array-post"), pipelineContext)
    }
  }
}
