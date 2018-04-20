package com.acxiom.pipeline.drivers

import java.nio.file.{Files, Path}
import java.util.Properties

import com.acxiom.pipeline._
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class KafkaPipelineDriverSuiteTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val kafkaLogs: Path = Files.createTempDirectory("kafkaLogs")
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  val testingServer = new TestingServer
  val kafkaProperties = new Properties()
  kafkaProperties.put("broker.id", "0")
  kafkaProperties.put("port", "9092")
  kafkaProperties.put("zookeeper.connect", testingServer.getConnectString)
  kafkaProperties.put("host.name", "127.0.0.1")
  kafkaProperties.put("auto.create.topics.enable", "true")
  kafkaProperties.put(KafkaConfig.LogDirsProp, kafkaLogs.toFile.getAbsolutePath)
  val server = new KafkaServerStartable(new KafkaConfig(kafkaProperties))

  override def beforeAll(): Unit = {
    testingServer.start()
    server.startup()

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = new SparkConf()
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
        .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      "org.apache.hadoop.io.compress.ZFramedCodec,org.apache.hadoop.io." +
        "compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
        "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
        "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

    SparkTestHelper.sparkSession = SparkSession.builder().config(SparkTestHelper.sparkConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    server.shutdown()
    server.awaitShutdown()
    testingServer.stop()

    SparkTestHelper.sparkSession.sparkContext.cancelAllJobs()
    SparkTestHelper.sparkSession.sparkContext.stop()
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
    // cleanup the kafka directories
    FileUtils.deleteDirectory(kafkaLogs.toFile)
  }

  describe("Kafka Pipeline Driver") {
    it("Should process simple records from Kafka") {
      When("5 kafaka messages are posted")
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("acks", "all")
      props.put("retries", "0")
      props.put("batch.size", "16384")
      props.put("linger.ms", "1")
      props.put("buffer.memory", "33554432")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      val dataRows = List(List("1", "2", "3", "4", "5"),
        List("6", "7", "8", "9", "10"),
        List("11", "12", "13", "14", "15"),
        List("16", "17", "18", "19", "20"),
        List("21", "22", "23", "24", "25"))
      val topic = "TEST"
      dataRows.foreach(row =>
        producer.send(new ProducerRecord[String, String](topic, "InboundRecord", row.mkString("|"))))

      producer.flush()
      producer.close()

      var executionComplete = false
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
          assert(pipelines.lengthCompare(1) == 0)
          val params = pipelineContext.parameters.getParametersByPipelineId("1")
          assert(params.isDefined)
          assert(params.get.parameters.contains("PROCESS_KAFKA_DATA"))
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse]
            .primaryReturn.getOrElse(false).asInstanceOf[Boolean])
          executionComplete = true
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable => fail(s"Pipeline Failed to run: ${t.getMessage}")
          }
        }
      }

      And("the kafka spark listener is running")
      val args = List("--driverSetupClass", "com.acxiom.pipeline.drivers.SparkTestDriverSetup", "--pipeline", "basic",
        "--globalInput", "global-input-value", "--topics", topic, "--kafkaNodes", "localhost:9092",
        "--terminationPeriod", "5000", "--fieldDelimiter", "|", "--duration-type", "seconds",
      "--duration", "1")
      new KafkaPipelineDriver().main(args.toArray)
      Then("5 records should be processed")
      assert(executionComplete)
    }
  }
}

// TODO These objects should be moved out of this suite at some point to be reused by other suites
object SparkTestHelper {
  val MASTER = "local[2]"
  val APPNAME = "file-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineListener: PipelineListener = _

  val MESSAGE_PROCESSING_STEP = PipelineStep(Some("PROCESS_KAFKA_DATA"), Some("Parses Kafka data"), None, Some("Pipeline"),
    Some(List(Parameter(Some("string"), Some("dataFrame"), Some(true), None, Some("!initialDataFrame")))),
    Some(EngineMeta(Some("MockTestSteps.processIncomingData"))), None)
  val BASIC_PIPELINE = List(Pipeline(Some("1"), Some("Basic Pipeline"), Some(List(MESSAGE_PROCESSING_STEP))))
}

case class SparkTestDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  val ctx = PipelineContext(Some(SparkTestHelper.sparkConf), Some(SparkTestHelper.sparkSession), Some(parameters),
    PipelineSecurityManager(),
    PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
    Some(if (parameters.contains("stepPackages")) {
      parameters("stepPackages").asInstanceOf[String]
        .split(",").toList
    } else {
      List("com.acxiom.pipeline.steps", "com.acxiom.pipeline.drivers")
    }),
    PipelineStepMapper(),
    Some(SparkTestHelper.pipelineListener),
    Some(SparkTestHelper.sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))

  override def pipelines: List[Pipeline] = {
    parameters.getOrElse("pipeline", "basic") match {
      case "basic" => SparkTestHelper.BASIC_PIPELINE
    }
  }

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = ctx
}

object MockTestSteps {
  val ZERO = 0
  val ONE = 1
  val FIVE = 5

  def processIncomingData(dataFrame: DataFrame, pipelineContext: PipelineContext): PipelineStepResponse = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
    val count = dataFrame.count()
    if (count != FIVE) {
      pipelineContext.addStepMessage(PipelineStepMessage(s"Row count was wrong $count", stepId, pipelineId, PipelineStepMessageType.error))
    }

    val fieldDelimiter = pipelineContext.getGlobalString("fieldDelimiter").getOrElse(",")
    val stepMessages = pipelineContext.stepMessages.get
    dataFrame.foreach(row => {
      val columns = row.getString(ONE).split(fieldDelimiter.toCharArray()(0))
      if (columns.lengthCompare(FIVE) != ZERO) {
        stepMessages.add(PipelineStepMessage(s"Column count was wrong: ${columns.length}",
          stepId, pipelineId, PipelineStepMessageType.error))
      }
    })

    PipelineStepResponse(Some(true), None)
  }
}
