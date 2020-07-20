package com.acxiom.kafka

import java.nio.file.{Files, Path}
import java.util.Properties

import com.acxiom.kafka.drivers.KafkaPipelineDriver
import com.acxiom.kafka.steps.KafkaSteps
import com.acxiom.pipeline._
import com.acxiom.pipeline.drivers.{DriverSetup, StreamingDataParser}
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

import scala.collection.JavaConverters._

class KafkaSuiteTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val kafkaLogs: Path = Files.createTempDirectory("kafkaLogs")
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  val testingServer = new TestingServer
  val kafkaProperties = new Properties()
  kafkaProperties.put("broker.id", "0")
  kafkaProperties.put("port", "9092")
  kafkaProperties.put("zookeeper.connect", testingServer.getConnectString)
  kafkaProperties.put("host.name", "127.0.0.1")
  kafkaProperties.put("auto.create.topics.enable", "true")
  kafkaProperties.put("offsets.topic.replication.factor", "1")
  kafkaProperties.put(KafkaConfig.LogDirsProp, kafkaLogs.toFile.getAbsolutePath)
  val server = new KafkaServerStartable(new KafkaConfig(kafkaProperties))
  val dataRows = List(List("1", "2", "3", "4", "5"),
    List("6", "7", "8", "9", "10"),
    List("11", "12", "13", "14", "15"),
    List("16", "17", "18", "19", "20"),
    List("21", "22", "23", "24", "25"))
  val kafkaProducerProperties = new Properties()
  private val KAFKA_NODES = "localhost:9092"
  kafkaProducerProperties.put("bootstrap.servers", KAFKA_NODES)
  kafkaProducerProperties.put("acks", "all")
  kafkaProducerProperties.put("retries", "0")
  kafkaProducerProperties.put("batch.size", "16384")
  kafkaProducerProperties.put("linger.ms", "1")
  kafkaProducerProperties.put("buffer.memory", "33554432")
  kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  override def beforeAll(): Unit = {
    testingServer.start()
    server.startup()

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    Logger.getLogger("com.acxiom.kafka").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = new SparkConf()
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
        .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      ",org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
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
      When("5 kafka messages are posted")
      val topic = sendKafkaMessages("|", Some("col1"))
      var executionComplete = false
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          assert(pipelines.lengthCompare(1) == 0)
          val params = pipelineContext.parameters.getParametersByPipelineId("1")
          assert(params.isDefined)
          assert(params.get.parameters.contains("PROCESS_KAFKA_DATA"))
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse]
            .primaryReturn.getOrElse(false).asInstanceOf[Boolean])
          executionComplete = true
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable => fail(s"Pipeline Failed to run: ${t.getMessage}")
          }
        }
      }

      And("the kafka spark listener is running")
      val args = List("--driverSetupClass", "com.acxiom.kafka.SparkTestDriverSetup", "--pipeline", "basic",
        "--globalInput", "global-input-value", "--topics", topic, "--kafkaNodes", KAFKA_NODES,
        "--terminationPeriod", "5000", "--fieldDelimiter", "|", "--duration-type", "seconds",
        "--duration", "1", "--expectedCount", "5", "--expectedAttrCount", "5")
      KafkaPipelineDriver.main(args.toArray)
      Then("5 records should be processed")
      assert(executionComplete)
    }

    it("Should process records using alternate data parsers") {
      When("5 kafka messages are posted with comma delimiter")
      val topic = sendKafkaMessages(",", None, true)
      var executionComplete = false
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          assert(pipelines.lengthCompare(1) == 0)
          val params = pipelineContext.parameters.getParametersByPipelineId("1")
          assert(params.isDefined)
          assert(params.get.parameters.contains("PROCESS_KAFKA_DATA"))
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse]
            .primaryReturn.getOrElse(false).asInstanceOf[Boolean])
          executionComplete = true
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable => fail(s"Pipeline Failed to run: ${t.getMessage}")
          }
        }
      }

      And("the kafka spark listener is running expecting either comma or pipe delimiter")
      val args = List("--driverSetupClass", "com.acxiom.kafka.SparkTestDriverSetup", "--pipeline", "parser",
        "--globalInput", "global-input-value", "--topics", topic, "--kafkaNodes", KAFKA_NODES,
        "--terminationPeriod", "5000", "--fieldDelimiter", "|", "--duration-type", "seconds", "--duration", "1",
        "--streaming-parsers", "com.acxiom.kafka.TestKafkaStreamingDataParserPipe,com.acxiom.kafka.TestKafkaStreamingDataParserComma",
        "--expectedCount", "5", "--expectedAttrCount", "5")
      KafkaPipelineDriver.main(args.toArray)
      Then("5 records should be processed")
      assert(executionComplete)
    }

    it("Should process records using default data parser") {
      When("5 kafka messages are posted with other delimiter")
      val topic = sendKafkaMessages(":", Some("col1"))
      var executionComplete = false
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          assert(pipelines.lengthCompare(1) == 0)
          val params = pipelineContext.parameters.getParametersByPipelineId("1")
          assert(params.isDefined)
          assert(params.get.parameters.contains("PROCESS_KAFKA_DATA"))
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse]
            .primaryReturn.getOrElse(false).asInstanceOf[Boolean])
          executionComplete = true
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable => fail(s"Pipeline Failed to run: ${t.getMessage}")
          }
        }
      }

      And("the kafka spark listener is running expecting either comma or pipe delimiter")
      val args = List("--driverSetupClass", "com.acxiom.kafka.SparkTestDriverSetup", "--pipeline", "parser",
        "--globalInput", "global-input-value", "--topics", topic, "--kafkaNodes", KAFKA_NODES,
        "--terminationPeriod", "5000", "--fieldDelimiter", "|", "--duration-type", "seconds", "--duration", "1",
        "--streaming-parsers", "com.acxiom.kafka.TestKafkaStreamingDataParserPipe,com.acxiom.kafka.TestKafkaStreamingDataParserComma",
        "--expectedCount", "5", "--expectedAttrCount", "3")
      KafkaPipelineDriver.main(args.toArray)
      Then("5 records should be processed with 3 attributes")
      assert(executionComplete)
    }

    it("Should process simple records using custom data parser") {
      When("5 kafka messages are posted")
      val topic = sendKafkaMessages("|")
      var executionComplete = false
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          assert(pipelines.lengthCompare(1) == 0)
          val params = pipelineContext.parameters.getParametersByPipelineId("1")
          assert(params.isDefined)
          assert(params.get.parameters.contains("PROCESS_KAFKA_DATA"))
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse]
            .primaryReturn.getOrElse(false).asInstanceOf[Boolean])
          executionComplete = true
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable => fail(s"Pipeline Failed to run: ${t.getMessage}")
          }
        }
      }

      And("the kafka spark listener is running")
      val args = List("--driverSetupClass", "com.acxiom.kafka.SparkTestDriverSetup", "--pipeline", "parser",
        "--globalInput", "global-input-value", "--topics", topic, "--kafkaNodes", KAFKA_NODES,
        "--terminationPeriod", "5000", "--fieldDelimiter", "|", "--duration-type", "seconds", "--duration", "1", "--streaming-parsers",
        "com.acxiom.kafka.TestKafkaStreamingDataParserPipe,com.acxiom.kafka.TestKafkaStreamingDataParserPipe",
        "--expectedCount", "5", "--expectedAttrCount", "5")
      KafkaPipelineDriver.main(args.toArray)
      Then("5 records should be processed")
      assert(executionComplete)
    }
  }

  private def sendKafkaMessages(delimiter: String, keyField: Option[String] = None, usePostMessage: Boolean = false) = {
    val topic = "TEST"
    if (usePostMessage) {
      dataRows.foreach(row => KafkaSteps.postMessage(row.mkString(delimiter), topic, KAFKA_NODES, "InboundRecord"))
    } else {
      val df = SparkTestHelper.sparkSession.createDataFrame(dataRows.map(r => Row(r: _*)).asJava,
        StructType(Seq(
          StructField("col1", StringType),
          StructField("col2", StringType),
          StructField("col3", StringType),
          StructField("col4", StringType),
          StructField("col5", StringType))))
      if (keyField.isDefined) {
        KafkaSteps.writeToStreamByKeyField(df, topic, KAFKA_NODES, keyField.get, delimiter)
      } else {
        KafkaSteps.writeToStreamByKey(df, topic, KAFKA_NODES, "InboundRecord", delimiter)
      }
    }
    topic
  }
}

// TODO These objects should be moved out of this suite at some point to be reused by other suites
object SparkTestHelper {
  val MASTER = "local[2]"
  val APPNAME = "file-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineListener: PipelineListener = _

  val MESSAGE_PROCESSING_STEP: PipelineStep = PipelineStep(Some("PROCESS_KAFKA_DATA"), Some("Parses Kafka data"), None, Some("Pipeline"),
    Some(List(Parameter(Some("string"), Some("dataFrame"), Some(true), None, Some("!initialDataFrame")))),
    Some(EngineMeta(Some("MockTestSteps.processIncomingData"))), None)
  val COMPLEX_MESSAGE_PROCESSING_STEP: PipelineStep = PipelineStep(Some("PROCESS_KAFKA_DATA"), Some("Parses Kafka data"), None, Some("Pipeline"),
    Some(List(Parameter(Some("string"), Some("dataFrame"), Some(true), None, Some("!initialDataFrame")))),
    Some(EngineMeta(Some("MockTestSteps.processIncomingMessage"))), None)
  val BASIC_PIPELINE = List(Pipeline(Some("1"), Some("Basic Pipeline"), Some(List(MESSAGE_PROCESSING_STEP))))
  val PARSER_PIPELINE = List(Pipeline(Some("1"), Some("Parser Pipeline"), Some(List(COMPLEX_MESSAGE_PROCESSING_STEP))))
}

class TestKafkaStreamingDataParserPipe extends StreamingDataParser[ConsumerRecord[String, String]] {
  override def canParse(rdd: RDD[ConsumerRecord[String, String]]): Boolean = {
    !rdd.map(r => r.value.contains("|")).collect.contains(false)
  }

  override def parseRDD(rdd: RDD[ConsumerRecord[String, String]], sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(rdd.map(r => Row(r.value().split('|'): _*)),
      StructType(List(StructField("col1", StringType),
        StructField("col2", StringType),
        StructField("col3", StringType),
        StructField("col4", StringType),
        StructField("col5", StringType)))).toDF()
  }
}

class TestKafkaStreamingDataParserComma extends StreamingDataParser[ConsumerRecord[String, String]] {
  override def canParse(rdd: RDD[ConsumerRecord[String, String]]): Boolean = {
    !rdd.map(r => r.value.contains(",")).collect.contains(false)
  }

  override def parseRDD(rdd: RDD[ConsumerRecord[String, String]], sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(rdd.map(r => Row(r.value().split(','): _*)),
      StructType(List(StructField("col1", StringType),
        StructField("col2", StringType),
        StructField("col3", StringType),
        StructField("col4", StringType),
        StructField("col5", StringType)))).toDF()
  }
}

case class SparkTestDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  val ctx: PipelineContext = PipelineContext(Some(SparkTestHelper.sparkConf), Some(SparkTestHelper.sparkSession), Some(parameters),
    PipelineSecurityManager(),
    PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
    Some(if (parameters.contains("stepPackages")) {
      parameters("stepPackages").asInstanceOf[String]
        .split(",").toList
    } else {
      List("com.acxiom.pipeline.steps", "com.acxiom.kafka")
    }),
    PipelineStepMapper(),
    Some(SparkTestHelper.pipelineListener),
    Some(SparkTestHelper.sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))

  override def pipelines: List[Pipeline] = {
    parameters.getOrElse("pipeline", "basic") match {
      case "basic" => SparkTestHelper.BASIC_PIPELINE
      case "parser" => SparkTestHelper.PARSER_PIPELINE
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
    val expectedCount = pipelineContext.getGlobalString("expectedCount").getOrElse("0").toLong
    val count = dataFrame.count()
    if (count != expectedCount) {
      pipelineContext.addStepMessage(PipelineStepMessage(s"Row count was wrong $count, expected=$expectedCount", stepId, pipelineId,
        PipelineStepMessageType.error))
    }

    val fieldDelimiter = pipelineContext.getGlobalString("fieldDelimiter").getOrElse(",")
    val stepMessages = pipelineContext.stepMessages.get
    val expectedAttrCount = pipelineContext.getGlobalString("expectedAttrCount").getOrElse("0").toInt
    dataFrame.foreach(row => {
      val columns = row.getString(ONE).split(fieldDelimiter.toCharArray()(0))
      if (columns.lengthCompare(expectedAttrCount) != ZERO) {
        stepMessages.add(PipelineStepMessage(s"Column count was wrong,expected=$expectedAttrCount,actual=${columns.length}",
          stepId, pipelineId, PipelineStepMessageType.error))
      }
    })

    assert(dataFrame.select("topic").distinct().collect()(0).getString(ZERO) == pipelineContext.globals.get("topics"))

    PipelineStepResponse(Some(true), None)
  }

  def processIncomingMessage(dataFrame: DataFrame, pipelineContext: PipelineContext): PipelineStepResponse = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
    val count = dataFrame.count()

    val expectedCount = pipelineContext.getGlobalString("expectedCount").getOrElse("0").toLong
    if (count != expectedCount) {
      pipelineContext.addStepMessage(PipelineStepMessage(s"Row count was wrong $count, expected=$expectedCount", stepId, pipelineId,
        PipelineStepMessageType.error))
    }

    val expectedAttrCount = pipelineContext.getGlobalString("expectedAttrCount").getOrElse("0").toInt
    val stepMessages = pipelineContext.stepMessages.get
    dataFrame.foreach(row => {
      val columnLength = row.length
      if (columnLength != expectedAttrCount) {
        stepMessages.add(PipelineStepMessage(s"Column count was wrong,expected=$expectedAttrCount,actual=$columnLength",
          stepId, pipelineId, PipelineStepMessageType.error))
      }
    })

    PipelineStepResponse(Some(true), None)
  }
}
