package com.acxiom.kafka

import com.acxiom.kafka.pipeline.KafkaPipelineListener
import com.acxiom.kafka.pipeline.connectors.KafkaDataConnector
import com.acxiom.kafka.steps.KafkaSteps
import com.acxiom.pipeline._
import com.acxiom.pipeline.drivers.{DefaultPipelineDriver, DriverSetup}
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.Properties
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

class KafkaSuiteTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {
  implicit val formats: Formats = DefaultFormats
  private val expectedEvents = List(("executionStarted", 1), ("executionFinished", 1), ("pipelineStarted", 1),
    ("pipelineFinished", 1), ("pipelineStepStarted", 1), ("pipelineStepFinished", 1),
    ("registerStepException", 1), ("executionStopped", 1))
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
  private val dataRows = List(List("1", "2", "3", "4", "5"),
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

  private val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_NODES)
  kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-client-id")
  kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

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

    if (!SparkTestHelper.sparkSession.sparkContext.isStopped) {
      SparkTestHelper.sparkSession.sparkContext.cancelAllJobs()
      SparkTestHelper.sparkSession.sparkContext.stop()
    }
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
      val testListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          assert(pipelines.lengthCompare(1) == 0)
          val params = pipelineContext.parameters.getParametersByPipelineId("1")
          assert(params.isDefined)
          assert(params.get.parameters.contains("PROCESS_KAFKA_DATA"))
          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
          // TODO Switched return from boolean to StreamingQuery
          //          assert(params.get.parameters("PROCESS_KAFKA_DATA").asInstanceOf[PipelineStepResponse]
          //            .primaryReturn.getOrElse(false).asInstanceOf[Boolean])
          executionComplete = true
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable => fail(s"Pipeline Failed to run: ${t.getMessage}")
          }
        }
      }
      val kafkaListener = new KafkaPipelineListener("test-pipeline-messages", "", None.orNull,
        "status", KAFKA_NODES, "test-client-id")
      SparkTestHelper.pipelineListener = CombinedPipelineListener(List[PipelineListener](testListener, kafkaListener))

      And("the kafka spark listener is running")
      val args = List("--driverSetupClass", "com.acxiom.kafka.SparkTestDriverSetup", "--pipeline", "basic",
        "--globalInput", "global-input-value", "--topics", "TEST", "--kafkaNodes", KAFKA_NODES,
        "--fieldDelimiter", "|", "--expectedCount", "5", "--expectedAttrCount", "5",
        "--STREAMING_QUERY_TIMEOUT_MS", "3000")
      DefaultPipelineDriver.main(args.toArray)
      Then("5 records should be processed")
      assert(executionComplete)
      // Verify that Kafka received the messages
      val consumer = new KafkaConsumer[String, String](kafkaConsumerProperties)
      // Subscribe to the topic.
      consumer.subscribe(List("status").asJavaCollection)
      pollConsumer(consumer, Constants.TEN)
      consumer.close(Duration.ofMillis(500))
    }

    it("Should fail and not retry") {
      val topic = sendKafkaMessages("|", Some("col1"))
      SparkTestHelper.pipelineListener = new PipelineListener {}
      val args = List("--driverSetupClass", "com.acxiom.kafka.SparkTestDriverSetup", "--pipeline", "errorTest",
        "--globalInput", "global-input-value", "--topics", topic, "--kafkaNodes", KAFKA_NODES,
        "--terminationPeriod", "500", "--fieldDelimiter", "|", "--duration-type", "seconds",
        "--duration", "1", "--expectedCount", "5", "--expectedAttrCount", "5", "--terminateAfterFailures", "true")
      val thrown = intercept[RuntimeException] {
        DefaultPipelineDriver.main(args.toArray)
      }
      assert(thrown.getMessage.startsWith("Failed to process execution plan after 1 attempts"))
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

  @tailrec
  private def pollConsumer(consumer: KafkaConsumer[String, String], expectedCount: Int): Unit = {
    val records = consumer.poll(Duration.ofMillis(2000))
    if (records.asScala.toList.nonEmpty) {
      val eventList = records.asScala.toList.foldLeft(List[String]())((events, record) => {
        val map = JsonMethods.parse(record.value()).extract[Map[String, Any]]
        events :+ map("event").asInstanceOf[String]
      })
      assert(eventList.map(expectedEvents.toMap.getOrElse(_, 0)).sum == expectedCount)
    } else {
      pollConsumer(consumer, expectedCount)
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

  val DATA_CONNECTOR_STEP: PipelineStep = PipelineStep(Some("LOAD_DATA_CONNECTOR"), Some("Load Data From Kafka"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("connector"), Some(true), None, Some("!kafkaConnector")))),
    Some(EngineMeta(Some("DataConnectorSteps.loadDataFrame"))), Some("PROCESS_KAFKA_DATA"))
  val MESSAGE_PROCESSING_STEP: PipelineStep = PipelineStep(Some("PROCESS_KAFKA_DATA"), Some("Parses Kafka data"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("@LOAD_DATA_CONNECTOR")))),
    Some(EngineMeta(Some("MockTestSteps.processIncomingData"))), None)
  val STREAMING_MONITOR_STEP: PipelineStep = PipelineStep(Some("STREAMING_MONITOR"), Some("Monitors the streaming queries"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("query"), Some(true), None, Some("@PROCESS_KAFKA_DATA")))),
    Some(EngineMeta(Some("FlowUtilsSteps.monitorStreamingQuery"))), None)

  val COMPLEX_MESSAGE_PROCESSING_STEP: PipelineStep = PipelineStep(Some("PROCESS_KAFKA_DATA"), Some("Parses Kafka data"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("!initialDataFrame")))),
    Some(EngineMeta(Some("MockTestSteps.processIncomingMessage"))), None)
  val ERROR_STEP: PipelineStep = PipelineStep(Some("THROW_ERROR"), Some("Throws an error"), None, Some("Pipeline"),
    Some(List()), Some(EngineMeta(Some("MockTestSteps.throwError"))), None)
  val BRANCH_STEP: PipelineStep = PipelineStep(Some("BRANCH_LOGIC"), Some("Determines Pipeline Step"), None, Some("branch"),
    Some(List(Parameter(`type` = Some("text"), name = Some("value"), value = Some("!passTest || false")),
      Parameter(`type` = Some("result"), name = Some("true"), value = Some("PROCESS_KAFKA_DATA")),
      Parameter(`type` = Some("result"), name = Some("false"), value = Some("THROW_ERROR")))),
    Some(EngineMeta(Some("MockTestSteps.parrotStep"))), None)
  val BASIC_PIPELINE: List[Pipeline] = List(Pipeline(Some("1"), Some("Basic Pipeline"),
    Some(List(DATA_CONNECTOR_STEP, MESSAGE_PROCESSING_STEP.copy(nextStepId = Some("STREAMING_MONITOR")), STREAMING_MONITOR_STEP))))
  val PARSER_PIPELINE: List[Pipeline] = List(Pipeline(Some("1"), Some("Parser Pipeline"), Some(List(COMPLEX_MESSAGE_PROCESSING_STEP))))
  val ERROR_PIPELINE: List[Pipeline] = List(Pipeline(Some("1"), Some("Error Pipeline"), Some(List(BRANCH_STEP, ERROR_STEP, MESSAGE_PROCESSING_STEP))))
}

case class SparkTestDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  private val kafkaConnector = KafkaDataConnector(parameters("topics").asInstanceOf[String],
    parameters("kafkaNodes").asInstanceOf[String], None, None, "KafaConnector", None, None)
  val ctx: PipelineContext = PipelineContext(Some(SparkTestHelper.sparkConf), Some(SparkTestHelper.sparkSession),
    Some(parameters + ("kafkaConnector" -> kafkaConnector)),
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
      case "errorTest" => SparkTestHelper.ERROR_PIPELINE
    }
  }

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = ctx
}

object MockTestSteps {
  val ZERO = 0
  val ONE = 1
  val FIVE = 5

  def parrotStep(value: Any): String = value.toString

  def throwError(pipelineContext: PipelineContext): Any = {
    throw PipelineException(message = Some("This step should not be called"),
      context = Some(pipelineContext),
      pipelineProgress = Some(pipelineContext.getPipelineExecutionInfo))
  }

  def processIncomingData(dataFrame: DataFrame, pipelineContext: PipelineContext): PipelineStepResponse = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
    val expectedCount = pipelineContext.getGlobalString("expectedCount").getOrElse("0").toLong
    val topic = pipelineContext.getGlobalString("topics").getOrElse("")
    var count = 0

    val fieldDelimiter = pipelineContext.getGlobalString("fieldDelimiter").getOrElse(",")
    val stepMessages = pipelineContext.stepMessages.get
    val expectedAttrCount = pipelineContext.getGlobalString("expectedAttrCount").getOrElse("0").toInt
    implicit val encoder: ExpressionEncoder[Row] = org.apache.spark.sql.catalyst.encoders.RowEncoder(StructType(Seq(
      StructField("key", StringType),
      StructField("value", StringType),
      StructField("topic", StringType),
      StructField("partition", IntegerType),
      StructField("offset", LongType),
      StructField("timestamp", TimestampType),
      StructField("timestampType", IntegerType))))
    val messages = pipelineContext.stepMessages.get
    val df = dataFrame.mapPartitions(rows => {
      rows.foreach(row => {
        val columns = new String(row.get(ONE).asInstanceOf[Array[Byte]]).split(fieldDelimiter.toCharArray()(0))
        assert(row.getString(Constants.TWO) == topic)
        if (columns.lengthCompare(expectedAttrCount) != ZERO) {
          stepMessages.add(PipelineStepMessage(s"Column count was wrong,expected=$expectedAttrCount,actual=${columns.length}",
            stepId, pipelineId, PipelineStepMessageType.error))
        }
        count += 1
      })
      if (count != expectedCount) {
        messages.add(PipelineStepMessage(s"Row count was wrong $count, expected=$expectedCount", stepId, pipelineId,
          PipelineStepMessageType.error))
      }
      rows
    })

    // Start the Streaming Query here
    PipelineStepResponse(Some(df.writeStream.format("memory")
      .outputMode("update")
      .queryName("test")
      .option("truncate", "false").start()), None)
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
