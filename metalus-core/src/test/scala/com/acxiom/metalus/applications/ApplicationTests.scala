package com.acxiom.metalus.applications

import com.acxiom.metalus.Constants.{NINE, TWELVE}
import com.acxiom.metalus._
import com.acxiom.metalus.api.BasicAuthorization
import com.acxiom.metalus.applications.Color.Color
import com.acxiom.metalus.context.Json4sContext
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.parser.JsonParser.StepSerializer
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, urlPathEqualTo}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.json4s.JsonAST.JString
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JObject}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.net.HttpURLConnection
import java.nio.file.Files
import scala.io.Source

class ApplicationTests extends AnyFunSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    Logger.getLogger("com.acxiom.metalus").setLevel(Level.DEBUG)
  }

  describe("ApplicationUtils - Parsing") {
    // Load the application for all the tests
    val application = JsonParser.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-test.json")).mkString)
    it("Should create an application") {
      val pipelineContext = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "")), None, PipelineListener())
      verifyApplication(application, pipelineContext)
    }

    it("Should create an execution plan from a REST response") {
      val application = JsonParser.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-response-test.json")).mkString)
      val pipelineContext = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "")), None, PipelineListener())
      verifyApplication(application, pipelineContext)
    }

    it("Should throw an exception if pipelineId is not provided") {
      val thrown = intercept[IllegalArgumentException] {
        val badApplication = JsonParser.parseApplication(
          Source.fromInputStream(getClass.getResourceAsStream("/no-pipeline-application-test.json")).mkString
        )
        val pipelineContext = ApplicationUtils.createPipelineContext(badApplication, Some(Map[String, Any]("rootLogLevel" -> true)), None)
      }
      assert(thrown.getMessage == "Application pipelineId is required!")
    }

    it("Should load custom json4s formats from the application json") {
      val app = JsonParser.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-json4s-test.json")).mkString)
      val ctx = ApplicationUtils.createPipelineContext(app, Some(Map[String, Any]("rootLogLevel" -> true,
        "customLogLevels" -> "")), None, PipelineListener())
      assert(ctx.globals.isDefined)
      val global = ctx.getGlobal("coop")
      assert(global.isDefined)
      assert(global.get.isInstanceOf[Coop])
      val coop = global.get.asInstanceOf[Coop]
      assert(coop.name == "chicken-coop")
      assert(coop.color == Color.WHITE)
      assert(coop.chickens.exists(c => c.breed == "silkie" && c.color == Color.BUFF))
      assert(coop.chickens.exists(c => c.breed == "polish" && c.color == Color.BLACK))
    }

    it("Should support EnumIdSerialization") {
      val app = JsonParser.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-enum-test.json")).mkString)
      val ctx = ApplicationUtils.createPipelineContext(app, Some(Map[String, Any]("rootLogLevel" -> true,
        "customLogLevels" -> "")), None, PipelineListener())
      assert(ctx.globals.isDefined)
      val global = ctx.getGlobal("silkie")
      assert(global.isDefined)
      assert(global.get.isInstanceOf[Silkie])
      assert(global.get.asInstanceOf[Silkie].color == Color.GOLD)
    }
  }

  describe("ApplicationDriverSetup") {
    val applicationJson = Source.fromInputStream(getClass.getResourceAsStream("/application-test.json")).mkString
    it("Should load and create an Application from the parameters") {
      val setup = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson, "rootLogLevel" -> "OFF",
        "customLogLevels" -> ""))
      verifyApplication(setup.application, setup.pipelineContext)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
    }

    it("Should load and create an Application from a local file") {
      val testDirectory = Files.createTempDirectory("localApplicationTests")
      val file = new File(testDirectory.toFile, "application-test.json")
      // Create the JSON file
      val outputStream = new OutputStreamWriter(new FileOutputStream(file))
      outputStream.write(applicationJson)
      outputStream.flush()
      outputStream.close()

      val setup = ApplicationDriverSetup(Map[String, Any](
        "applicationConfigPath" -> file.getAbsolutePath,
        "rootLogLevel" -> "ERROR",
        "customLogLevels" -> "com.test:INFO,com.test1:DEBUG"))
      verifyApplication(setup.application, setup.pipelineContext)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
      assert(Logger.getLogger("com.test").getLevel.toString == "INFO")
      assert(Logger.getLogger("com.test1").getLevel.toString == "DEBUG")

      file.delete()
      FileUtils.deleteDirectory(testDirectory.toFile)
    }

    // TODO [2.0 Review] WIll core support HDFS or will it be a separate library?
//    it("Should load and create an Application from an HDFS file") {
//      val testDirectory = Files.createTempDirectory("hdfsApplicationTests")
//      val config = new HdfsConfiguration()
//      config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDirectory.toFile.getAbsolutePath)
//      val miniCluster = new MiniDFSCluster.Builder(config).build()
//      miniCluster.waitActive()
//      val fs = miniCluster.getFileSystem
//
//      // Update the application JSON so Spark uses the HDFS cluster
//      val application = JsonParser.parseApplication(applicationJson)
//      val options =
//        Map[String, String]("name" -> "spark.hadoop.fs.defaultFS",
//          "value" -> miniCluster.getFileSystem().getUri.toString) :: application.sparkConf.get("setOptions").asInstanceOf[List[Map[String, String]]]
//
//      // Create the JSON file on HDFS
//      implicit val formats: Formats = DefaultFormats
//      val applicationJSONPath = new Path("hdfs:///application-test.json")
//      val outputStream = new OutputStreamWriter(fs.create(applicationJSONPath))
//      outputStream.write(Serialization.write(application.copy(sparkConf = Some(conf))))
//      outputStream.flush()
//      outputStream.close()
//
//      assert(fs.exists(applicationJSONPath))
//      val setup = ApplicationDriverSetup(Map[String, Any](
//        "applicationConfigPath" -> "hdfs:///application-test.json",
//        "applicationConfigurationLoader" -> "com.acxiom.metalus.fs.HDFSFileManager",
//        "dfs-cluster" -> miniCluster.getFileSystem().getUri.toString,
//        "rootLogLevel" -> "INFO",
//        "customLogLevels" -> ""))
//      verifyApplication(setup.application, setup.pipelineContext)
//      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
//      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
//      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
//
//      miniCluster.shutdown(true)
//      FileUtils.deleteDirectory(testDirectory.toFile)
//    }

    it("Should load and create an Application from an http location") {
      val wireMockServer = new WireMockServer("15432".toInt)
      wireMockServer.start()

      wireMockServer.addStubMapping(get(urlPathEqualTo("/applications/12345"))
        .withBasicAuth("cli-user", "cli-password")
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)
          .withHeader("content-type", "application/json")
          .withBody(applicationJson)
        ).build())

      val setup = ApplicationDriverSetup(Map[String, Any](
        "applicationConfigPath" -> s"${wireMockServer.baseUrl()}/applications/12345",
        "rootLogLevel" -> "ERROR",
        "customLogLevels" -> "",
        "authorization.class" -> "com.acxiom.metalus.api.BasicAuthorization",
        "authorization.username" -> "cli-user",
        "authorization.password" -> "cli-password"))
      verifyApplication(setup.application, setup.pipelineContext)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))

      implicit val formats: Formats = DefaultFormats + new StepSerializer
      val application = JsonParser.parseJson(applicationJson, "com.acxiom.metalus.applications.Application").asInstanceOf[Application]

      val json = org.json4s.native.Serialization.write(ApplicationResponse(application))

      wireMockServer.addStubMapping(get(urlPathEqualTo("/applications/54321"))
        .willReturn(aResponse()
          .withStatus(HttpURLConnection.HTTP_OK)
          .withHeader("content-type", "application/json")
          .withBody(json)
        ).build())

      val setup1 = ApplicationDriverSetup(Map[String, Any](
        "applicationConfigPath" -> s"${wireMockServer.baseUrl()}/applications/54321",
        "rootLogLevel" -> "ERROR",
        "customLogLevels" -> ""))
      verifyApplication(setup1.application, setup1.pipelineContext)
      assert(!setup1.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup1.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup1.pipelineContext.globals.get.contains("applicationConfigurationLoader"))

      wireMockServer.stop()
    }

    it("Should create an application from jar metadata") {
      val setup = ApplicationDriverSetup(Map[String, Any](
        "applicationId" -> "testApp",
        "rootLogLevel" -> "ERROR",
        "customLogLevels" -> "com.test:INFO,com.test1:DEBUG"))
      verifyApplication(setup.application, setup.pipelineContext)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
      assert(Logger.getLogger("com.test").getLevel.toString == "INFO")
      assert(Logger.getLogger("com.test1").getLevel.toString == "DEBUG")
    }

    // TODO [2.0 Review] Move to the spark project
//    it("Should respect the 'enableHiveSupport' parameter") {
//      val thrown = intercept[IllegalArgumentException] {
//        val ads = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson,
//          "rootLogLevel" -> "OFF",
//          "enableHiveSupport" -> true)
//        )
//        ads.executionPlan
//      }
//      assert(thrown.getMessage == "Unable to instantiate SparkSession with Hive support because Hive classes are not found.")
//    }

    it("Should refresh an application") {
      val setup = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson,
        "rootLogLevel" -> "DEBUG",
        "customLogLevels" -> ""))
      verifyApplication(setup.application, setup.refreshContext(setup.pipelineContext))
    }

    it("Should detect a missing parameter") {
      val thrown = intercept[RuntimeException] {
        val ads = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson, "logLevel" -> "TRACE"))
        ads.pipelineContext
      }
      assert(thrown.getMessage.contains("Missing required parameters: rootLogLevel"))
    }

    it("Should fail when config information is not present") {
      val thrown = intercept[RuntimeException] {
        val ads = ApplicationDriverSetup(Map[String, Any]("rootLogLevel" -> "DEBUG"))
        ads.pipelineContext
      }
      assert(Option(thrown).isDefined)
      assert(thrown.getMessage == "Either the applicationId, applicationJson or the" +
        " applicationConfigPath/applicationConfigurationLoader parameters must be provided!")
    }
  }

  describe("ApplicationUtils - Json4s Formats") {
    it("Should parse formats") {
      val jsonString =
        """{
          |  "customSerializers": [
          |    {
          |      "className": "com.acxiom.metalus.applications.ChickenSerializer"
          |    }
          |  ],
          |  "enumNameSerializers": [
          |    {
          |      "className": "com.acxiom.metalus.applications.Color"
          |    }
          |  ],
          |  "hintSerializers": [
          |    {
          |      "className": "com.acxiom.metalus.applications.Child1"
          |    },
          |    {
          |      "className": "com.acxiom.metalus.applications.Child2"
          |    }
          |  ]
          |}""".stripMargin
      val serializers = JsonParser.parseJson(jsonString, "com.acxiom.metalus.applications.Json4sSerializers")(DefaultFormats).asInstanceOf[Json4sSerializers]
      assert(Option(serializers).isDefined)
      assert(serializers.customSerializers.isDefined)
      assert(serializers.customSerializers.get.nonEmpty)
      assert(serializers.enumNameSerializers.isDefined)
      assert(serializers.enumNameSerializers.get.nonEmpty)
      val jsonDoc =
        """[
          |    {
          |      "breed": "silkie",
          |      "color": "BUFF"
          |    },
          |    {
          |      "breed": "polish",
          |      "color": "BLACK"
          |    }
          |  ]""".stripMargin

      implicit val formats: Formats = new Json4sContext().generateFormats(Some(serializers))
      val chickenList = parse(jsonDoc).extract[List[Chicken]]
      assert(Option(chickenList).isDefined)
      assert(chickenList.nonEmpty)
      assert(chickenList.head.isInstanceOf[Silkie])
      assert(chickenList(1).isInstanceOf[Polish])

      val roots =
        """{
          |"child1": {
          |    "jsonClass": "com.acxiom.metalus.applications.Child1",
          |    "name": "child1"
          |  },
          |  "child2": {
          |    "jsonClass": "com.acxiom.metalus.applications.Child2",
          |    "name": "child2"
          |  }
          |}""".stripMargin
      val rootMap = parse(roots).extract[Map[String, Any]]
      assert(rootMap.nonEmpty)
      assert(rootMap("child1").isInstanceOf[Child1])
      assert(rootMap("child2").isInstanceOf[Child2])
    }
  }

  private def verifyApplication(application: Application, pipelineContext: PipelineContext) = {
    assert(pipelineContext.globals.get.contains("authorization"))
    assert(pipelineContext.globals.get("authorization").isInstanceOf[BasicAuthorization])
    assert(pipelineContext.globals.get("authorization").asInstanceOf[BasicAuthorization].username == "myuser")
    assert(pipelineContext.globals.get("authorization").asInstanceOf[BasicAuthorization].password == "mypassword")
    assert(pipelineContext.getGlobal("plainObject").isDefined)
    assert(pipelineContext.getGlobal("plainObject").get.isInstanceOf[List[Map[String, Any]]])
    assert(pipelineContext.getGlobalAs[List[Map[String, Any]]]("plainObject").get.head("some") == "value")
    assert(pipelineContext.getGlobalAs[List[Map[String, Any]]]("plainObject").get.head("another") == 5)

    assert(application.pipelineId.getOrElse("") == "root")
    assert(application.pipelineTemplates.length == 6)

    val rootPipeline = pipelineContext.pipelineManager.getPipeline("root")
    assert(rootPipeline.isDefined)
    assert(rootPipeline.get.steps.isDefined)
    val steps = rootPipeline.get.steps.get
    assert(steps.size == 14)

    // Verify the proper steps
    assert(steps.head.id.get == "SPLIT")
    assert(steps.head.`type`.get == "split")
    assert(steps.head.isInstanceOf[PipelineStep])
    assert(steps.head.asInstanceOf[PipelineStep].params.isDefined)
    assert(steps.head.asInstanceOf[PipelineStep].params.get.length == 3)

    assert(steps(1).id.getOrElse("") == "EXE_0_COND")
    assert(steps(1).`type`.getOrElse("") == "step-group")
    assert(steps(1).isInstanceOf[PipelineStepGroup])
    assert(steps(1).asInstanceOf[PipelineStepGroup].pipelineId.getOrElse("") == "Pipeline1a")
    assert(steps(2).id.getOrElse("") == "EXE_0_BRANCH")
    assert(steps(2).`type`.getOrElse("") == "branch")

    // First execution
    assert(steps(3).isInstanceOf[PipelineStepGroup])
    verifyExe0StepGroup(steps(3).asInstanceOf[PipelineStepGroup], pipelineContext)

    // Second execution
    assert(steps(4).id.getOrElse("") == "EXE_1_COND")
    assert(steps(4).`type`.getOrElse("") == "step-group")
    assert(steps(4).isInstanceOf[PipelineStepGroup])
    assert(steps(4).asInstanceOf[PipelineStepGroup].pipelineId.getOrElse("") == "Pipeline2")
    assert(steps(5).id.getOrElse("") == "EXE_1_BRANCH")
    assert(steps(5).`type`.getOrElse("") == "branch")
    val execution2 = steps(6)
    assert(execution2.isInstanceOf[PipelineStepGroup])
    verifyExe1StepGroup(execution2.asInstanceOf[PipelineStepGroup], pipelineContext)

    // Third Execution
    assert(steps(7).id.getOrElse("") == "EXE_2_FORK")
    assert(steps(7).`type`.getOrElse("") == "fork")
    assert(steps(7).params.isDefined)
    assert(steps(7).params.get.length == 2)
    assert(steps(7).params.get(0).name.getOrElse("") == "forkByValues")
    assert(steps(7).params.get(0).value.getOrElse("") == "doesn't_matter_in_this_test")
    assert(steps(7).params.get(1).name.getOrElse("") == "forkMethod")
    assert(steps(7).params.get(1).value.getOrElse("") == "parallel")
    assert(steps(10).id.getOrElse("") == "EXE_2_JOIN")
    assert(steps(10).`type`.getOrElse("") == "join")
    assert(steps(8).isInstanceOf[PipelineStepGroup])
    assert(steps(9).isInstanceOf[PipelineStepGroup])
    verifyExe2StepGroups(steps(8).asInstanceOf[PipelineStepGroup], steps(9).asInstanceOf[PipelineStepGroup], pipelineContext)

    // Fourth Execution
    assert(steps(11).isInstanceOf[PipelineStepGroup])
    assert(steps(12).isInstanceOf[PipelineStepGroup])
    verifyExe3StepGroups(steps(11).asInstanceOf[PipelineStepGroup], steps(12).asInstanceOf[PipelineStepGroup], pipelineContext)
  }

  private def verifyExe0StepGroup(stepGroup: PipelineStepGroup, pipelineContext: PipelineContext) = {
    // Ensure the id is set properly
    assert(stepGroup.id.getOrElse("") == "EXE_0")
    assert(stepGroup.pipelineId.getOrElse("") == "9ecbaee7-ba8d-4520-815b-e5e5a24b1872")
    // Use the global pipeline parameters
    assert(pipelineContext.parameters.length == 1)
    assert(pipelineContext.parameters.head.parameters.getOrElse("fred", "") == "johnson")
    // Use the global step mapper
    assert(pipelineContext.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Test Step Mapper")
    // TODO [2.0 Review] Ensure no parents
    // Verify the globals object was properly constructed
    val globals = pipelineContext.globals.get
    val globalCount = if (globals.contains("authorization.class")) {
      TWELVE
    } else {
      NINE
    }
    assert(globals.size == globalCount)
    assert(globals.contains("rootLogLevel"))
    // Verify the overrides which will reside in the step-group parameters
    assert(stepGroup.params.isDefined)
    assert(stepGroup.params.get.nonEmpty)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "number").isDefined)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "number").get.value.getOrElse("").isInstanceOf[BigInt])
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "number").get.value.getOrElse("").asInstanceOf[BigInt] == 2)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "float").isDefined)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "float").get.value.getOrElse("").isInstanceOf[Double])
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "float").get.value.getOrElse("").asInstanceOf[Double] == 3.5)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "string").isDefined)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "string").get.value.getOrElse("").isInstanceOf[String])
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "string").get.value.getOrElse("").toString == "sub string")
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "stringList").isDefined)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "stringList").get.value.getOrElse("").isInstanceOf[List[String]])
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "stringList").get.value.getOrElse("").asInstanceOf[List[String]].head == "someString")
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "mappedObject").isDefined)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "mappedObject").get.value.getOrElse("").isInstanceOf[Map[String, Any]])
    val subGlobalObject = stepGroup.params.get.find(_.name.getOrElse("") == "mappedObject")
      .get.value.get.asInstanceOf[Map[String, Any]]("object").asInstanceOf[Map[String, Any]]
    assert(subGlobalObject.getOrElse("name", "") == "Execution Mapped Object")
    assert(subGlobalObject.contains("subObjects"))
    val subObjects = subGlobalObject.getOrElse("subObjects", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]
    assert(subObjects.length == 3)
    // These are maps not objects
    assert(subObjects.head.getOrElse("name", "") == "Sub Object 1a")
    assert(subObjects(1).getOrElse("name", "") == "Sub Object 2a")
    assert(subObjects(2).getOrElse("name", "") == "Sub Object 3")
    // Verify the PipelineParameters object was set properly
    assert(pipelineContext.parameters.length == 1)
    assert(pipelineContext.hasPipelineParameters("Pipeline1", "fred"))
    assert(pipelineContext.getParameterByPipelineKey("Pipeline1", "fred").getOrElse("") == "johnson")
  }

  private def verifyExe1StepGroup(stepGroup: PipelineStepGroup, pipelineContext: PipelineContext) = {
    assert(stepGroup.id.getOrElse("") == "EXE_1")
    assert(stepGroup.pipelineId.getOrElse("") == "Pipeline2_Override")
    val pipeline = pipelineContext.pipelineManager.getPipeline(stepGroup.pipelineId.get)
    assert(pipeline.isDefined)
    verifyMappedParameter(pipeline.get)
    val globals1 = pipelineContext.globals.get
    val globalCount = if (globals1.contains("authorization.class")) {
      TWELVE
    } else {
      NINE
    }
    assert(globals1.size == globalCount)
    assert(globals1.contains("rootLogLevel"))
    assert(globals1.contains("rootLogLevel"))
    assert(globals1.contains("number"))
    assert(globals1("number").asInstanceOf[BigInt] == 1)
    assert(globals1.contains("float"))
    assert(globals1("float").asInstanceOf[Double] == 1.5)
    assert(globals1.contains("string"))
    assert(globals1("string").asInstanceOf[String] == "some string")
    assert(globals1.contains("mappedObject"))
    val subGlobalObject1 = globals1("mappedObject").asInstanceOf[TestGlobalObject]
    assert(subGlobalObject1.name.getOrElse("") == "Global Mapped Object")
    assert(subGlobalObject1.subObjects.isDefined)
    assert(subGlobalObject1.subObjects.get.length == 2)
    assert(subGlobalObject1.subObjects.get.head.name.getOrElse("") == "Sub Object 1")
    assert(subGlobalObject1.subObjects.get(1).name.getOrElse("") == "Sub Object 2")
    assert(globals1.contains("listMappedObjects"))
    val listGlobalObjects = globals1("listMappedObjects").asInstanceOf[List[TestGlobalObject]]
    assert(listGlobalObjects.nonEmpty)
    val listGlobalObject = listGlobalObjects.head
    assert(listGlobalObject.name.getOrElse("") == "Global Mapped Object in a list")
    assert(listGlobalObject.subObjects.isDefined)
    assert(listGlobalObject.subObjects.get.length == 2)
    assert(listGlobalObject.subObjects.get.head.name.getOrElse("") == "Sub Object 1")
    assert(listGlobalObject.subObjects.get(1).name.getOrElse("") == "Sub Object 2")
    assert(stepGroup.params.isDefined)
    assert(stepGroup.params.get.nonEmpty)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "howard").isDefined)
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "howard").get.value.getOrElse("").isInstanceOf[String])
    assert(stepGroup.params.get.find(_.name.getOrElse("") == "howard").get.value.getOrElse("").asInstanceOf[String] == "johnson")
  }

  private def verifyExe2StepGroups(stepGroup1: PipelineStepGroup, stepGroup2: PipelineStepGroup, pipelineContext: PipelineContext) = {
    // Ensure the ids are set properly
    assert(stepGroup1.id.getOrElse("") == "EXE_2_1")
    assert(stepGroup2.id.getOrElse("") == "EXE_2_2")
    // Validate correct pipeline values
    val pipeline1 = pipelineContext.pipelineManager.getPipeline(stepGroup1.pipelineId.getOrElse(""))
    assert(pipeline1.isDefined)
    assert(pipeline1.get.steps.get.head.params.get.filter(pa => pa.name.get == "value")
      .head.value.get == "!mappedObject2")
    val pipeline2 = pipelineContext.pipelineManager.getPipeline(stepGroup2.pipelineId.getOrElse(""))
    assert(pipeline2.isDefined)
    assert(pipeline2.get.steps.get.head.params.get.filter(pa => pa.name.get == "value")
      .head.value.get == "CHICKEN")
    // Verify the parameters are properly constructed
    assert(stepGroup1.params.isDefined)
    assert(stepGroup1.params.get.nonEmpty)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "number").isDefined)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "number").get.value.getOrElse("").isInstanceOf[BigInt])
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "number").get.value.getOrElse("").asInstanceOf[BigInt] == 2)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "float").isDefined)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "float").get.value.getOrElse("").isInstanceOf[Double])
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "float").get.value.getOrElse("").asInstanceOf[Double] == 3.5)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "string").isDefined)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "string").get.value.getOrElse("").isInstanceOf[String])
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "string").get.value.getOrElse("").toString == "sub string")
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "mappedObject").isDefined)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "mappedObject").get.value.getOrElse("").isInstanceOf[Map[String, Any]])
    val subGlobalObject = stepGroup1.params.get.find(_.name.getOrElse("") == "mappedObject")
      .get.value.get.asInstanceOf[Map[String, Any]]("object").asInstanceOf[Map[String, Any]]
    assert(subGlobalObject.getOrElse("name", "") == "Execution Mapped Object")
    assert(subGlobalObject.contains("subObjects"))
    val subObjects = subGlobalObject.getOrElse("subObjects", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]
    assert(subObjects.length == 3)
    // These are maps not objects
    assert(subObjects.head.getOrElse("name", "") == "Sub Object 1a")
    assert(subObjects(1).getOrElse("name", "") == "Sub Object 2a")
    assert(subObjects(2).getOrElse("name", "") == "Sub Object 3")
  }

  private def verifyExe3StepGroups(stepGroup1: PipelineStepGroup, stepGroup2: PipelineStepGroup, pipelineContext: PipelineContext) = {
    // Ensure the ids are set properly
    assert(stepGroup1.id.getOrElse("") == "EXE_3_1")
    assert(stepGroup2.id.getOrElse("") == "EXE_3_2")
    // Validate correct pipeline values
    val pipeline1 = pipelineContext.pipelineManager.getPipeline(stepGroup1.pipelineId.getOrElse(""))
    assert(pipeline1.isDefined)
    assert(pipeline1.get.steps.get.head.params.get.filter(pa => pa.name.get == "value").head.value.get == "!mappedObject")
    val pipeline2 = pipelineContext.pipelineManager.getPipeline(stepGroup2.pipelineId.getOrElse(""))
    assert(pipeline2.isDefined)
    assert(pipeline2.get.steps.get.head.params.get.filter(pa => pa.name.get == "value").head.value.get == "CHICKEN")

    assert(stepGroup1.params.isDefined)
    assert(stepGroup1.params.get.nonEmpty)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "number").isDefined)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "number").get.value.getOrElse("").isInstanceOf[BigInt])
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "number").get.value.getOrElse("").asInstanceOf[BigInt] == 5)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "newThing").isDefined)
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "newThing").get.value.getOrElse("").isInstanceOf[String])
    assert(stepGroup1.params.get.find(_.name.getOrElse("") == "newThing").get.value.getOrElse("").toString == "Chickens rule!")
    assert(stepGroup2.params.get.find(_.name.getOrElse("") == "useParentGlobals").isDefined)
    assert(stepGroup2.params.get.find(_.name.getOrElse("") == "useParentGlobals").get.value.getOrElse("").isInstanceOf[Boolean])
    assert(stepGroup2.params.get.find(_.name.getOrElse("") == "useParentGlobals").get.value.getOrElse("").asInstanceOf[Boolean])
  }

  private def verifyMappedParameter(pipeline: Pipeline) = {
    implicit val formats: Formats = DefaultFormats
    assert(pipeline.steps.isDefined)
    assert(pipeline.steps.get.head.params.isDefined)
    assert(pipeline.steps.get.head.params.get.length == 2)
    assert(pipeline.steps.get.head.params.get(1).name.getOrElse("") == "parameterObject")
    assert(pipeline.steps.get.head.params.get(1).className.getOrElse("") == "com.acxiom.metalus.applications.TestGlobalObject")
    assert(pipeline.steps.get.head.params.get(1).value.isDefined)
    assert(pipeline.steps.get.head.params.get(1).value.get.isInstanceOf[Map[String, Any]])
    val mappedParameter = JsonParser.parseJson(
      Serialization.write(pipeline.steps.get.head.params.get(1).value.get.asInstanceOf[Map[String, Any]]),
      pipeline.steps.get.head.params.get(1).className.getOrElse("")).asInstanceOf[TestGlobalObject]
    assert(mappedParameter.name.getOrElse("") == "Parameter Mapped Object")
    assert(mappedParameter.subObjects.isDefined)
    assert(mappedParameter.subObjects.get.length == 2)
    assert(mappedParameter.subObjects.get.head.name.getOrElse("") == "Param Object 1")
    assert(mappedParameter.subObjects.get(1).name.getOrElse("") == "Param Object 2")
  }

  // TODO [2.0 Review] Move to spark project
//  def verifyUDF(spark: SparkSession): Unit = {
//    val df = spark.sql("select chicken() as chicken")
//    assert(df.columns.length == 1)
//    assert(df.collect().head.getString(0) == "moo")
//  }
//
//  def verifyListeners(spark: SparkSession): Unit = {
//    val listeners = PackagePrivateSparkTestHelper.getSparkListeners(spark)
//      .filter(l => l.isInstanceOf[TestPipelineListener] || l.isInstanceOf[TestSparkListener])
//    assert(listeners.size == 4)
//    val pl = listeners.filter(_.isInstanceOf[TestPipelineListener]).map(_.asInstanceOf[TestPipelineListener])
//    val sl = listeners.filter(_.isInstanceOf[TestSparkListener]).map(_.asInstanceOf[TestSparkListener])
//    assert(pl.size == 2)
//    assert(pl.exists(_.name == "Test Pipeline Listener"))
//    assert(pl.exists(_.name == "Sub Pipeline Listener"))
//    assert(sl.size == 2)
//    assert(sl.exists(_.name == "Listener1"))
//    assert(sl.exists(_.name == "Listener2"))
//  }
}

case class TestPipelineListener(name: String) extends PipelineListener
//
//case class TestSparkListener(name: String) extends SparkListener

case class TestPipelineSecurityManager(name: String) extends PipelineSecurityManager

case class TestPipelineStepMapper(name: String) extends PipelineStepMapper

case class TestGlobalObject(name: Option[String], subObjects: Option[List[TestSubGlobalObject]])

case class TestSubGlobalObject(name: Option[String])

// TODO [2.0 Review] Move to spark project
//class TestUDF(name: String) extends PipelineUDF {
//  override def register(sparkSession: SparkSession, globals: Map[String, Any]): UserDefinedFunction = {
//    val func: () => String = { () => s"${globals.getOrElse(name, "moo")}" }
//    sparkSession.udf.register(name, func)
//  }
//}

object Color extends Enumeration {
  type Color = Value
  val BLACK, GOLD, BUFF, WHITE = Value
}

trait Chicken {
  val breed: String
  val color: Color
}

case class Silkie(color: Color) extends Chicken {
  override val breed: String = "silkie"
}

case class Polish(color: Color) extends Chicken {
  override val breed: String = "polish"
}

case class Coop(name: String, chickens: List[Chicken], color: Color)

class ChickenSerializer extends CustomSerializer[Chicken](f => ( {
  case json: JObject =>
    implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(Color) + new ChickenSerializer
    if ((json \ "breed") == JString("silkie")) {
      json.extract[Silkie]
    } else {
      json.extract[Polish]
    }
}, {
  case chicken: Chicken =>
    parse(s"""{"breed":"${chicken.breed},"color":"${chicken.color.toString}"}""")
}))

trait Root {
  def name: String
}

case class Child1(override val name: String) extends Root {}

case class Child2(override val name: String) extends Root {}
