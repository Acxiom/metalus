package com.acxiom.pipeline.applications

import com.acxiom.pipeline._
import com.acxiom.pipeline.api.BasicAuthorization
import com.acxiom.pipeline.applications.Color.Color
import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.DriverUtils
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, urlPathEqualTo}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.PackagePrivateSparkTestHelper
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.json4s.JsonAST.JString
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JObject}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.net.HttpURLConnection
import java.nio.file.Files
import scala.io.Source

class ApplicationTests extends FunSpec with BeforeAndAfterAll with Suite {

  private val SIX = 6
  private val SEVEN = 7
  private val NINE = 9
  private val TEN = 10
  private val TWELVE = 12
  private val THIRTEEN = 13

  override def beforeAll() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
  }

  describe("ApplicationUtils - Parsing") {
    // Load the application for all the tests
    val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-test.json")).mkString)
    it("Should create an execution plan") {
      val sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
        .setMaster("local")
      val executionPlan = ApplicationUtils.createExecutionPlan(application, Some(Map[String, Any]("rootLogLevel" -> true,
        "customLogLevels" -> "")), sparkConf,
        PipelineListener())
      verifyApplication(executionPlan)
      // Verify the evaluation pipelines
      assert(executionPlan.head.evaluationPipelines.isDefined)
      assert(executionPlan.head.evaluationPipelines.get.nonEmpty)
      assert(executionPlan.head.evaluationPipelines.get.head.id.getOrElse("NONE") == "Pipeline1a")
      assert(executionPlan.head.executionType == "pipeline")
      assert(executionPlan(1).evaluationPipelines.isDefined)
      assert(executionPlan(1).evaluationPipelines.get.nonEmpty)
      assert(executionPlan(1).evaluationPipelines.get.head.id.getOrElse("NONE") == "Pipeline2")
      assert(executionPlan(1).executionType == "pipeline")
      assert(executionPlan(2).forkByValue.isDefined)
      assert(executionPlan(2).forkByValue.get == "some.path.within.globals")
      assert(executionPlan(2).executionType == "fork")
      // Clean up
      removeSparkListeners(executionPlan.head.pipelineContext.sparkSession.get)
      executionPlan.head.pipelineContext.sparkSession.get.stop()
    }

    it("Should create an execution plan from a REST response") {
      val app = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-response-test.json")).mkString)
      val sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
        .setMaster("local")
      val executionPlan = ApplicationUtils.createExecutionPlan(app, Some(Map[String, Any]("rootLogLevel" -> true,
        "customLogLevels" -> "")), sparkConf,
        PipelineListener())
      verifyApplication(executionPlan)
      removeSparkListeners(executionPlan.head.pipelineContext.sparkSession.get)
      executionPlan.head.pipelineContext.sparkSession.get.stop()
    }

    it("Should throw an exception if no pipelines are provided") {
      val thrown = intercept[IllegalArgumentException] {
        val badApplication = ApplicationUtils.parseApplication(
          Source.fromInputStream(getClass.getResourceAsStream("/no-pipeline-application-test.json")).mkString
        )
        val sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
          .setMaster("local")
        val executionPlan = ApplicationUtils.createExecutionPlan(badApplication, Some(Map[String, Any]("rootLogLevel" -> true)), sparkConf)
        executionPlan.head.pipelineContext.sparkSession.get.stop()
      }
      removeSparkListeners(SparkSession.builder().getOrCreate())
      assert(thrown.getMessage == "Either pipelines, pipelineIds or application pipelines must be provided for an execution")
    }

    it("Should load custom json4s formats from the application json") {
      val app = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-json4s-test.json")).mkString)
      val sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
        .setMaster("local")
      val executionPlan = ApplicationUtils.createExecutionPlan(app, Some(Map[String, Any]("rootLogLevel" -> true,
        "customLogLevels" -> "")), sparkConf,
        PipelineListener())
      assert(executionPlan.nonEmpty)
      val ctx = executionPlan.head.pipelineContext
      assert(ctx.globals.isDefined)
      val global = ctx.getGlobal("coop")
      assert(global.isDefined)
      assert(global.get.isInstanceOf[Coop])
      val coop = global.get.asInstanceOf[Coop]
      assert(coop.name == "chicken-coop")
      assert(coop.color == Color.WHITE)
      assert(coop.chickens.exists(c => c.breed == "silkie" && c.color == Color.BUFF))
      assert(coop.chickens.exists(c => c.breed == "polish" && c.color == Color.BLACK))
      removeSparkListeners(executionPlan.head.pipelineContext.sparkSession.get)
      executionPlan.head.pipelineContext.sparkSession.get.stop()
    }

    it("Should support EnumIdSerialization") {
      val app = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-enum-test.json")).mkString)
      val sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
        .setMaster("local")
      val executionPlan = ApplicationUtils.createExecutionPlan(app, Some(Map[String, Any]("rootLogLevel" -> true,
        "customLogLevels" -> "")), sparkConf,
        PipelineListener())
      assert(executionPlan.nonEmpty)
      val ctx = executionPlan.head.pipelineContext
      assert(ctx.globals.isDefined)
      val global = ctx.getGlobal("silkie")
      assert(global.isDefined)
      assert(global.get.isInstanceOf[Silkie])
      assert(global.get.asInstanceOf[Silkie].color == Color.GOLD)
      removeSparkListeners(executionPlan.head.pipelineContext.sparkSession.get)
      executionPlan.head.pipelineContext.sparkSession.get.stop()
    }
  }

  describe("ApplicationDriverSetup") {
    val applicationJson = Source.fromInputStream(getClass.getResourceAsStream("/application-test.json")).mkString
    it("Should load and create an Application from the parameters") {
      val setup = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson, "rootLogLevel" -> "OFF",
        "customLogLevels" -> ""))
      verifyDriverSetup(setup)
      verifyApplication(setup.executionPlan.get)
      verifyListeners(setup.pipelineContext.sparkSession.get)
      verifyUDF(setup.pipelineContext.sparkSession.get)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup.pipelineContext.sparkSession.get.stop()
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
      verifyDriverSetup(setup)
      verifyApplication(setup.executionPlan.get)
      verifyListeners(setup.pipelineContext.sparkSession.get)
      verifyUDF(setup.pipelineContext.sparkSession.get)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
      assert(Logger.getLogger("com.test").getLevel.toString == "INFO")
      assert(Logger.getLogger("com.test1").getLevel.toString == "DEBUG")

      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup.pipelineContext.sparkSession.get.stop()
      file.delete()
      FileUtils.deleteDirectory(testDirectory.toFile)
    }

    it("Should load and create an Application from an HDFS file") {
      val testDirectory = Files.createTempDirectory("hdfsApplicationTests")
      val config = new HdfsConfiguration()
      config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDirectory.toFile.getAbsolutePath)
      val miniCluster = new MiniDFSCluster.Builder(config).build()
      miniCluster.waitActive()
      val fs = miniCluster.getFileSystem

      // Update the application JSON so Spark uses the HDFS cluster
      val application = ApplicationUtils.parseApplication(applicationJson)
      val options =
        Map[String, String]("name" -> "spark.hadoop.fs.defaultFS",
          "value" -> miniCluster.getFileSystem().getUri.toString) :: application.sparkConf.get("setOptions").asInstanceOf[List[Map[String, String]]]
      val conf = application.sparkConf.get + ("setOption" -> options)

      // Create the JSON file on HDFS
      implicit val formats: Formats = DefaultFormats
      val applicationJSONPath = new Path("hdfs:///application-test.json")
      val outputStream = new OutputStreamWriter(fs.create(applicationJSONPath))
      outputStream.write(Serialization.write(application.copy(sparkConf = Some(conf))))
      outputStream.flush()
      outputStream.close()

      assert(fs.exists(applicationJSONPath))
      val setup = ApplicationDriverSetup(Map[String, Any](
        "applicationConfigPath" -> "hdfs:///application-test.json",
        "applicationConfigurationLoader" -> "com.acxiom.pipeline.fs.HDFSFileManager",
        "dfs-cluster" -> miniCluster.getFileSystem().getUri.toString,
        "rootLogLevel" -> "INFO",
        "customLogLevels" -> ""))
      verifyDriverSetup(setup)
      verifyApplication(setup.executionPlan.get)
      verifyListeners(setup.pipelineContext.sparkSession.get)
      verifyUDF(setup.pipelineContext.sparkSession.get)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))

      miniCluster.shutdown(true)
      FileUtils.deleteDirectory(testDirectory.toFile)
      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup.pipelineContext.sparkSession.get.stop()
    }

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
        "authorization.class" -> "com.acxiom.pipeline.api.BasicAuthorization",
        "authorization.username" -> "cli-user",
        "authorization.password" -> "cli-password"))
      verifyDriverSetup(setup)
      verifyApplication(setup.executionPlan.get)
      verifyListeners(setup.pipelineContext.sparkSession.get)
      verifyUDF(setup.pipelineContext.sparkSession.get)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))

      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup.pipelineContext.sparkSession.get.stop()

      implicit val formats: Formats = DefaultFormats
      val application = DriverUtils.parseJson(applicationJson, "com.acxiom.pipeline.applications.Application").asInstanceOf[Application]
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
      verifyDriverSetup(setup1)
      verifyApplication(setup1.executionPlan.get)
      verifyListeners(setup1.pipelineContext.sparkSession.get)
      verifyUDF(setup1.pipelineContext.sparkSession.get)
      assert(!setup1.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup1.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup1.pipelineContext.globals.get.contains("applicationConfigurationLoader"))

      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup1.pipelineContext.sparkSession.get.stop()
      wireMockServer.stop()
    }

    it("Should create an application from jar metadata") {
      val setup = ApplicationDriverSetup(Map[String, Any](
        "applicationId" -> "testApp",
        "rootLogLevel" -> "ERROR",
        "customLogLevels" -> "com.test:INFO,com.test1:DEBUG"))
      verifyDriverSetup(setup)
      verifyApplication(setup.executionPlan.get)
      verifyListeners(setup.pipelineContext.sparkSession.get)
      verifyUDF(setup.pipelineContext.sparkSession.get)
      assert(!setup.pipelineContext.globals.get.contains("applicationJson"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigPath"))
      assert(!setup.pipelineContext.globals.get.contains("applicationConfigurationLoader"))
      assert(Logger.getLogger("com.test").getLevel.toString == "INFO")
      assert(Logger.getLogger("com.test1").getLevel.toString == "DEBUG")

      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup.pipelineContext.sparkSession.get.stop()
    }

    it("Should respect the 'enableHiveSupport' parameter") {
      val thrown = intercept[IllegalArgumentException] {
        val ads = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson,
          "rootLogLevel" -> "OFF",
          "enableHiveSupport" -> true)
        )
        ads.executionPlan
      }
      assert(thrown.getMessage == "Unable to instantiate SparkSession with Hive support because Hive classes are not found.")
    }

    it("Should refresh an application") {
      val setup = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson,
        "rootLogLevel" -> "DEBUG",
        "customLogLevels" -> ""))
      val executionPlan = setup.executionPlan.get
      verifyApplication(setup.refreshExecutionPlan(executionPlan))
      removeSparkListeners(setup.pipelineContext.sparkSession.get)
      setup.pipelineContext.sparkSession.get.stop()
    }

    it("Should detect a missing parameter") {
      val thrown = intercept[RuntimeException] {
        val ads = ApplicationDriverSetup(Map[String, Any]("applicationJson" -> applicationJson, "logLevel" -> "TRACE"))
        ads.executionPlan

      }
      assert(thrown.getMessage.contains("Missing required parameters: rootLogLevel"))
    }

    it("Should fail when config information is not present") {
      val thrown = intercept[RuntimeException] {
        val ads = ApplicationDriverSetup(Map[String, Any]("rootLogLevel" -> "DEBUG"))
        ads.executionPlan
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
          |      "className": "com.acxiom.pipeline.applications.ChickenSerializer"
          |    }
          |  ],
          |  "enumNameSerializers": [
          |    {
          |      "className": "com.acxiom.pipeline.applications.Color"
          |    }
          |  ],
          |  "hintSerializers": [
          |    {
          |      "className": "com.acxiom.pipeline.applications.Child1"
          |    },
          |    {
          |      "className": "com.acxiom.pipeline.applications.Child2"
          |    }
          |  ]
          |}""".stripMargin
      val serializers = DriverUtils.parseJson(jsonString, "com.acxiom.pipeline.applications.Json4sSerializers")(DefaultFormats).asInstanceOf[Json4sSerializers]
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
      implicit val formats: Formats = ApplicationUtils.getJson4sFormats(Some(serializers))
      val chickenList = parse(jsonDoc).extract[List[Chicken]]
      assert(Option(chickenList).isDefined)
      assert(chickenList.nonEmpty)
      assert(chickenList.head.isInstanceOf[Silkie])
      assert(chickenList(1).isInstanceOf[Polish])

      val roots =
        """{
          |"child1": {
          |    "jsonClass": "com.acxiom.pipeline.applications.Child1",
          |    "name": "child1"
          |  },
          |  "child2": {
          |    "jsonClass": "com.acxiom.pipeline.applications.Child2",
          |    "name": "child2"
          |  }
          |}""".stripMargin
      val rootMap = parse(roots).extract[Map[String, Any]]
      assert(rootMap.nonEmpty)
      assert(rootMap("child1").isInstanceOf[Child1])
      assert(rootMap("child2").isInstanceOf[Child2])
    }
  }

  private def verifyDriverSetup(driverSetup: DriverSetup): Unit = {
    assert(driverSetup.executionPlan.isDefined)
    assert(driverSetup.pipelines == List())
    assert(driverSetup.initialPipelineId == "")
    assert(driverSetup.pipelineContext == driverSetup.executionPlan.get.head.pipelineContext)
  }

  private def verifyApplication(executionPlan: List[PipelineExecution]) = {
    assert(executionPlan.length == 4)
    // First execution
    val execution1 = executionPlan.head
    verifyFirstExecution(execution1)

    // Second execution
    val execution2 = executionPlan(1)
    verifySecondExecution(execution2)
    assert(execution2.pipelineContext.globals.get.contains("authorization"))
    assert(execution2.pipelineContext.globals.get("authorization").isInstanceOf[BasicAuthorization])
    assert(execution2.pipelineContext.globals.get("authorization").asInstanceOf[BasicAuthorization].username == "myuser")
    assert(execution2.pipelineContext.globals.get("authorization").asInstanceOf[BasicAuthorization].password == "mypassword")
    assert(execution2.pipelineContext.getGlobal("plainObject").isDefined)
    assert(execution2.pipelineContext.getGlobal("plainObject").get.isInstanceOf[List[Map[String, Any]]])
    assert(execution2.pipelineContext.getGlobalAs[List[Map[String, Any]]]("plainObject").get.head("some") == "value")
    assert(execution2.pipelineContext.getGlobalAs[List[Map[String, Any]]]("plainObject").get.head("another") == 5)


    // Third Execution
    val execution3 = executionPlan(2)
    verifyThirdExecution(execution3)

    // Fourth Execution
    val execution4 = executionPlan(3)
    verifyFourthExecution(execution4)
  }

  private def verifyFourthExecution(execution4: PipelineExecution) = {
    val ctx3 = execution4.pipelineContext
    // Ensure the id is set properly
    assert(execution4.id == "3")
    // Use the global listener
    assert(ctx3.pipelineListener.get.isInstanceOf[CombinedPipelineListener])
    assert(ctx3.pipelineListener.get.asInstanceOf[CombinedPipelineListener].listeners.head
      .asInstanceOf[TestPipelineListener].name == "Test Pipeline Listener")
    // Use a custom security manager
    assert(ctx3.security.asInstanceOf[TestPipelineSecurityManager].name == "Sub Security Manager")
    // Use the global pipeline parameters
    assert(ctx3.parameters.parameters.length == 1)
    assert(ctx3.parameters.parameters.head.parameters.getOrElse("fred", "") == "johnson")
    // Use the global step mapper
    assert(ctx3.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Test Step Mapper")
    // Validate the correct pipelines are set
    assert(execution4.pipelines.length == 2)
    // Validate correct pipeline values
    assert(execution4.pipelines.filter(p => p.id.get == "Pipeline1")
      .head.steps.get.head.params.get.filter(pa => pa.name.get == "value")
      .head.value.get == "!mappedObject")
    assert(execution4.pipelines.filter(p => p.id.get == "Pipeline2")
      .head.steps.get.head.params.get.filter(pa => pa.name.get == "value")
      .head.value.get == "CHICKEN")
    assert(execution4.parents.isEmpty)
    // Verify the globals object was properly merged
    val globals = ctx3.globals.get
    val globalCount = if (globals.contains("authorization.class")) {
      THIRTEEN
    } else {
      TEN
    }
    assert(globals.size == globalCount)
    assert(globals.contains("rootLogLevel"))
    assert(globals.contains("rootLogLevel"))
    assert(globals.contains("number"))
    assert(globals("number").asInstanceOf[BigInt] == 5)
    assert(globals.contains("float"))
    assert(globals("float").asInstanceOf[Double] == 1.5)
    assert(globals.contains("string"))
    assert(globals("string").asInstanceOf[String] == "some string")
    assert(globals("newThing").asInstanceOf[String] == "Chickens rule!")
    assert(globals.contains("mappedObject"))
    val subGlobalObject = globals("mappedObject").asInstanceOf[TestGlobalObject]
    assert(subGlobalObject.name.getOrElse("") == "Global Mapped Object")
    assert(subGlobalObject.subObjects.isDefined)
    assert(subGlobalObject.subObjects.get.length == 2)
    assert(subGlobalObject.subObjects.get.head.name.getOrElse("") == "Sub Object 1")
    assert(subGlobalObject.subObjects.get(1).name.getOrElse("") == "Sub Object 2")
    // Verify the PipelineParameters object was set properly
    assert(ctx3.parameters.parameters.length == 1)
    assert(ctx3.parameters.parameters.length == 1)
    assert(ctx3.parameters.hasPipelineParameters("Pipeline1"))
    assert(ctx3.parameters.getParametersByPipelineId("Pipeline1").get.parameters.size == 1)
    assert(ctx3.parameters.getParametersByPipelineId("Pipeline1").get.parameters("fred").asInstanceOf[String] == "johnson")
  }

  private def verifyThirdExecution(execution3: PipelineExecution) = {
    val ctx3 = execution3.pipelineContext
    // Ensure the id is set properly
    assert(execution3.id == "2")
    // Use the global listener
    assert(ctx3.pipelineListener.get.isInstanceOf[CombinedPipelineListener])
    assert(ctx3.pipelineListener.get.asInstanceOf[CombinedPipelineListener].listeners.head
      .asInstanceOf[TestPipelineListener].name == "Test Pipeline Listener")
    // Use a custom security manager
    assert(ctx3.security.asInstanceOf[TestPipelineSecurityManager].name == "Sub Security Manager")
    // Use the global pipeline parameters
    assert(ctx3.parameters.parameters.length == 1)
    assert(ctx3.parameters.parameters.head.parameters.getOrElse("fred", "") == "johnson")
    // Use the global step mapper
    assert(ctx3.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Test Step Mapper")
    // Validate the correct pipelines are set
    assert(execution3.pipelines.length == 2)
    // Validate correct pipeline values
    assert(execution3.pipelines.filter(p => p.id.get == "Pipeline1")
      .head.steps.get.head.params.get.filter(pa => pa.name.get == "value")
      .head.value.get == "!mappedObject2")
    assert(execution3.pipelines.filter(p => p.id.get == "Pipeline2")
      .head.steps.get.head.params.get.filter(pa => pa.name.get == "value")
      .head.value.get == "CHICKEN")
    assert(execution3.parents.isEmpty)
    // Verify the globals object was properly constructed
    val globals = ctx3.globals.get
    val globalCount = if (globals.contains("authorization.class")) {
      NINE
    } else {
      SIX
    }
    assert(globals.size == globalCount)
    assert(globals.contains("rootLogLevel"))
    assert(globals.contains("rootLogLevel"))
    assert(globals.contains("number"))
    assert(globals("number").asInstanceOf[BigInt] == 2)
    assert(globals.contains("float"))
    assert(globals("float").asInstanceOf[Double] == 3.5)
    assert(globals.contains("string"))
    assert(globals("string").asInstanceOf[String] == "sub string")
    assert(globals.contains("mappedObject"))
    val subGlobalObject = globals("mappedObject").asInstanceOf[TestGlobalObject]
    assert(subGlobalObject.name.getOrElse("") == "Execution Mapped Object")
    assert(subGlobalObject.subObjects.isDefined)
    assert(subGlobalObject.subObjects.get.length == 3)
    assert(subGlobalObject.subObjects.get.head.name.getOrElse("") == "Sub Object 1a")
    assert(subGlobalObject.subObjects.get(1).name.getOrElse("") == "Sub Object 2a")
    assert(subGlobalObject.subObjects.get(2).name.getOrElse("") == "Sub Object 3")
    // Verify the PipelineParameters object was set properly
    assert(ctx3.parameters.parameters.length == 1)
    assert(ctx3.parameters.parameters.length == 1)
    assert(ctx3.parameters.hasPipelineParameters("Pipeline1"))
    assert(ctx3.parameters.getParametersByPipelineId("Pipeline1").get.parameters.size == 1)
    assert(ctx3.parameters.getParametersByPipelineId("Pipeline1").get.parameters("fred").asInstanceOf[String] == "johnson")
  }

  private def verifySecondExecution(execution2: PipelineExecution) = {
    val ctx2 = execution2.pipelineContext
    assert(execution2.id == "1")
    assert(ctx2.pipelineListener.get.isInstanceOf[TestPipelineListener])
    assert(ctx2.pipelineListener.get.asInstanceOf[TestPipelineListener].name == "Sub Pipeline Listener")
    assert(ctx2.security.asInstanceOf[TestPipelineSecurityManager].name == "Test Security Manager")
    assert(ctx2.parameters.parameters.length == 1)
    assert(ctx2.parameters.parameters.head.parameters.getOrElse("howard", "") == "johnson")
    assert(ctx2.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Sub Step Mapper")
    assert(execution2.pipelines.head.name.getOrElse("") == "Pipeline 2")
    verifyMappedParameter(execution2)
    assert(execution2.parents.isDefined)
    assert(execution2.parents.get.head == "0")
    val globals1 = ctx2.globals.get
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
    assert(ctx2.parameters.parameters.length == 1)
    assert(ctx2.parameters.parameters.length == 1)
    assert(ctx2.parameters.hasPipelineParameters("Pipeline2"))
    assert(ctx2.parameters.getParametersByPipelineId("Pipeline2").get.parameters.size == 1)
    assert(ctx2.parameters.getParametersByPipelineId("Pipeline2").get.parameters("howard").asInstanceOf[String] == "johnson")
  }

  private def verifyMappedParameter(execution2: PipelineExecution) = {
    implicit val formats: Formats = DefaultFormats
    assert(execution2.pipelines.head.steps.isDefined)
    assert(execution2.pipelines.head.steps.get.head.params.isDefined)
    assert(execution2.pipelines.head.steps.get.head.params.get.length == 2)
    assert(execution2.pipelines.head.steps.get.head.params.get(1).name.getOrElse("") == "parameterObject")
    assert(execution2.pipelines.head.steps.get.head.params.get(1).className.getOrElse("") == "com.acxiom.pipeline.applications.TestGlobalObject")
    assert(execution2.pipelines.head.steps.get.head.params.get(1).value.isDefined)
    assert(execution2.pipelines.head.steps.get.head.params.get(1).value.get.isInstanceOf[Map[String, Any]])
    val mappedParameter = DriverUtils.parseJson(
      Serialization.write(execution2.pipelines.head.steps.get.head.params.get(1).value.get.asInstanceOf[Map[String, Any]]),
      execution2.pipelines.head.steps.get.head.params.get(1).className.getOrElse("")).asInstanceOf[TestGlobalObject]
    assert(mappedParameter.name.getOrElse("") == "Parameter Mapped Object")
    assert(mappedParameter.subObjects.isDefined)
    assert(mappedParameter.subObjects.get.length == 2)
    assert(mappedParameter.subObjects.get.head.name.getOrElse("") == "Param Object 1")
    assert(mappedParameter.subObjects.get(1).name.getOrElse("") == "Param Object 2")
  }

  private def verifyFirstExecution(execution1: PipelineExecution) = {
    val ctx1 = execution1.pipelineContext
    // Ensure the id is set properly
    assert(execution1.id == "0")
    // Use the global listener
    assert(ctx1.pipelineListener.get.isInstanceOf[CombinedPipelineListener])
    assert(ctx1.pipelineListener.get.asInstanceOf[CombinedPipelineListener].listeners.head
      .asInstanceOf[TestPipelineListener].name == "Test Pipeline Listener")
    // Use a custom security manager
    assert(ctx1.security.asInstanceOf[TestPipelineSecurityManager].name == "Sub Security Manager")
    // Use the global pipeline parameters
    assert(ctx1.parameters.parameters.length == 1)
    assert(ctx1.parameters.parameters.head.parameters.getOrElse("fred", "") == "johnson")
    // Use the global step mapper
    assert(ctx1.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Test Step Mapper")
    // Validate the correct pipeline is set
    assert(execution1.pipelines.head.name.getOrElse("") == "Pipeline 1")
    // Ensure no parents
    assert(execution1.parents.isEmpty)
    // Verify the globals object was properly constructed
    val globals = ctx1.globals.get
    val globalCount = if (globals.contains("authorization.class")) {
      TEN
    } else {
      SEVEN
    }
    assert(globals.size == globalCount)
    assert(globals.contains("rootLogLevel"))
    assert(globals.contains("rootLogLevel"))
    assert(globals.contains("number"))
    assert(globals("number").asInstanceOf[BigInt] == 2)
    assert(globals.contains("float"))
    assert(globals("float").asInstanceOf[Double] == 3.5)
    assert(globals.contains("string"))
    assert(globals("string").asInstanceOf[String] == "sub string")
    assert(globals("stringList").isInstanceOf[List[String]])
    assert(globals("stringList").asInstanceOf[List[String]].head == "someString")
    assert(globals.contains("mappedObject"))
    val subGlobalObject = globals("mappedObject").asInstanceOf[TestGlobalObject]
    assert(subGlobalObject.name.getOrElse("") == "Execution Mapped Object")
    assert(subGlobalObject.subObjects.isDefined)
    assert(subGlobalObject.subObjects.get.length == 3)
    assert(subGlobalObject.subObjects.get.head.name.getOrElse("") == "Sub Object 1a")
    assert(subGlobalObject.subObjects.get(1).name.getOrElse("") == "Sub Object 2a")
    assert(subGlobalObject.subObjects.get(2).name.getOrElse("") == "Sub Object 3")
    // Verify the PipelineParameters object was set properly
    assert(ctx1.parameters.parameters.length == 1)
    assert(ctx1.parameters.parameters.length == 1)
    assert(ctx1.parameters.hasPipelineParameters("Pipeline1"))
    assert(ctx1.parameters.getParametersByPipelineId("Pipeline1").get.parameters.size == 1)
    assert(ctx1.parameters.getParametersByPipelineId("Pipeline1").get.parameters("fred").asInstanceOf[String] == "johnson")
  }

  def verifyUDF(spark: SparkSession): Unit = {
    val df = spark.sql("select chicken() as chicken")
    assert(df.columns.length == 1)
    assert(df.collect().head.getString(0) == "moo")
  }

  def verifyListeners(spark: SparkSession): Unit = {
    val listeners = PackagePrivateSparkTestHelper.getSparkListeners(spark)
      .filter(l => l.isInstanceOf[TestPipelineListener] || l.isInstanceOf[TestSparkListener])
    assert(listeners.size == 4)
    val pl = listeners.filter(_.isInstanceOf[TestPipelineListener]).map(_.asInstanceOf[TestPipelineListener])
    val sl = listeners.filter(_.isInstanceOf[TestSparkListener]).map(_.asInstanceOf[TestSparkListener])
    assert(pl.size == 2)
    assert(pl.exists(_.name == "Test Pipeline Listener"))
    assert(pl.exists(_.name == "Sub Pipeline Listener"))
    assert(sl.size == 2)
    assert(sl.exists(_.name == "Listener1"))
    assert(sl.exists(_.name == "Listener2"))
  }

  def removeSparkListeners(spark: SparkSession): Unit = {
    val listeners = PackagePrivateSparkTestHelper.getSparkListeners(spark)
      .filter(l => l.isInstanceOf[TestPipelineListener] || l.isInstanceOf[TestSparkListener])
    listeners.foreach(l => spark.sparkContext.removeSparkListener(l))
  }
}

case class TestPipelineListener(name: String) extends SparkListener with PipelineListener

case class TestSparkListener(name: String) extends SparkListener

case class TestPipelineSecurityManager(name: String) extends PipelineSecurityManager

case class TestPipelineStepMapper(name: String) extends PipelineStepMapper

case class TestGlobalObject(name: Option[String], subObjects: Option[List[TestSubGlobalObject]])

case class TestSubGlobalObject(name: Option[String])

class TestUDF(name: String) extends PipelineUDF {
  override def register(sparkSession: SparkSession, globals: Map[String, Any]): UserDefinedFunction = {
    val func: () => String = { () => s"${globals.getOrElse(name, "moo")}" }
    sparkSession.udf.register(name, func)
  }
}

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
