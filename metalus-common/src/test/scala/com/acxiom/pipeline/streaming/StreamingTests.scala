package com.acxiom.pipeline.streaming

import com.acxiom.pipeline._
import com.acxiom.pipeline.connectors.{DataConnectorUtilities, HDFSDataConnector}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions, FlowUtilsSteps, Schema}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import java.io.{File, OutputStreamWriter}
import java.net.{ServerSocket, Socket}
import java.nio.file.{Files, Path}
import java.util.Date
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

class StreamingTests  extends FunSpec with BeforeAndAfterAll {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  private val MASTER = "local[2]"
  private val APPNAME = "spark-streaming-steps-spark"
  private var sparkConf: SparkConf = _
  private var sparkSession: SparkSession = _
  private val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  private var pipelineContext: PipelineContext = _
  var config: HdfsConfiguration = _
  var fs: FileSystem = _
  var miniCluster: MiniDFSCluster = _
  val file = new File(sparkLocalDir.toFile.getAbsolutePath, "cluster")

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    // set up mini hadoop cluster
    config = new HdfsConfiguration()
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, file.getAbsolutePath)
    miniCluster = new MiniDFSCluster.Builder(config).build()
    miniCluster.waitActive()
    // Only pull the fs object from the mini cluster
    fs = miniCluster.getFileSystem

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
      // Force Spark to use the HDFS cluster
      .set("spark.hadoop.fs.defaultFS", miniCluster.getFileSystem().getUri.toString)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()
    miniCluster.shutdown()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("Streaming Monitor") {
    it("Should run a continuous stream") {
      // Output path
      val path = miniCluster.getURI + "/metalus/data/socket_continuous.parquet"
      // 3 second timeout
      val ctx = pipelineContext.setGlobal("STREAMING_QUERY_TIMEOUT_MS", "5000")
      val port = Random.nextInt(Constants.ONE_HUNDRED) + Constants.NINE_THOUSAND
      // Input
      val load = getReadStream(port, None)
      // Output
      val writeOptions = DataFrameWriterOptions(saveMode = "append")
      // Setup the server
      val server = new ServerSocket(port)
      // Start the query
      val query = Some(DataConnectorUtilities.buildDataStreamWriter(load, writeOptions, path).start())
      // Write Data
      val socket = sendRecords(server, Constants.FIVE * Constants.TEN)
      // Thread the step
      val future = Future {
        FlowUtilsSteps.monitorStreamingQuery(query, None, ctx)
      }
      val f = Await.ready(future, Duration.Inf)
      assert(f.isCompleted)
      socket.close()
      // Verify that anything within the path is a file unless it is the _spark_metadata directory
      fs.listStatus(new org.apache.hadoop.fs.Path(path)).foreach(status => {
        assert(!status.isDirectory || status.getPath.toString == s"$path/_spark_metadata")
      })
      val hdfs = HDFSDataConnector("TestConnector", None, None)
      val df = hdfs.load(Some(path), ctx)
      assert(df.count() == 50)
    }

    it("Should run a batch partitioned stream") {
      // Output path
      val path = miniCluster.getURI + "/metalus/data/socket_partitioned.parquet"
      val checkpointLocation = s"${miniCluster.getURI}/metalus/data/streaming_partitioned_checkpoint"
      val ctx = pipelineContext.setGlobal("STREAMING_BATCH_MONITOR_TYPE", "count")
        .setGlobal("STREAMING_BATCH_MONITOR_COUNT", Constants.TEN)
        .setGlobal("STREAMING_BATCH_PARTITION_TEMPLATE", "date")
        .setGlobal("STREAMING_BATCH_PARTITION_GLOBAL", "PARTITION_VALUE")
        .setGlobal("PARTITION_VALUE", Constants.FILE_APPEND_DATE_FORMAT.format(new Date()))
      val port = Random.nextInt(Constants.ONE_HUNDRED) + Constants.NINE_THOUSAND
      // Output
      val writeOptions = DataFrameWriterOptions(saveMode = "append", partitionBy = Some(List("partition_column")),
        options = Some(Map("checkpointLocation" -> checkpointLocation)))
      // Setup the server
      val server = new ServerSocket(port)
      val dataFrame = getReadStream(port, Some("partition_column"), Some(Constants.FILE_APPEND_DATE_FORMAT.format(new Date())))
      // Start the query
      val query = Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, path).start())
      // Write Data
      val socket = sendRecords(server, Constants.TEN)
      val monitor = Some("com.acxiom.pipeline.streaming.BatchPartitionedStreamingQueryMonitor")
      // Thread the step
      val future = Future {
        val response = FlowUtilsSteps.monitorStreamingQuery(query, monitor, ctx)
        socket.close()
        assert(fs.exists(new org.apache.hadoop.fs.Path(checkpointLocation)))
        // Verify that everything within the path is a directory
        fs.listStatus(new org.apache.hadoop.fs.Path(path)).foreach(status => {
          assert(status.isDirectory)
        })
        if (response.primaryReturn.getOrElse("continue") == "continue") {
          // Delete the data because socket source doesn't have offsets
          fs.delete(new org.apache.hadoop.fs.Path(path), true)
          fs.delete(new org.apache.hadoop.fs.Path(checkpointLocation), true)
          val q1 = Some(DataConnectorUtilities.buildDataStreamWriter(
            getReadStream(port, Some("partition_column"),
              Some(Constants.FILE_APPEND_DATE_FORMAT.format(new Date()))), writeOptions, path).start())
          val s1 = sendRecords(server, Constants.TWENTY)
          val r = FlowUtilsSteps.monitorStreamingQuery(q1, monitor, ctx)
          s1.close()
          r
        } else {
          response
        }
      }
      val f = Await.ready(future, Duration.Inf)
      assert(f.isCompleted)
      server.close()
      // Verify that everything within the path is a directory
      fs.listStatus(new org.apache.hadoop.fs.Path(path)).foreach(status => {
        assert(status.isDirectory)
      })
      assert(fs.exists(new org.apache.hadoop.fs.Path(checkpointLocation)))
      val hdfs = HDFSDataConnector("TestConnector", None, None)
      val readOptions = DataFrameReaderOptions(schema = Some(Schema.fromStructType(dataFrame.schema)))
      val df = hdfs.load(Some(path), ctx, readOptions)
      assert(df.count() == Constants.TWENTY)
    }

    it("Should run a batch file stream") {
      // Output path
      val path = miniCluster.getURI + "/metalus/data/socket_file.parquet"
      val checkpointLocation = s"${miniCluster.getURI}/metalus/data/streaming_file_checkpoint"
      val ctx = pipelineContext.setGlobal("STREAMING_BATCH_MONITOR_TYPE", "count")
        .setGlobal("STREAMING_BATCH_MONITOR_COUNT", Constants.TEN)
        .setGlobal("STREAMING_BATCH_OUTPUT_TEMPLATE", "date")
        .setGlobal("STREAMING_BATCH_OUTPUT_GLOBAL", "destinationPath")
        .setGlobal("STREAMING_BATCH_OUTPUT_PATH_KEY", "socket_file")
        .setGlobal("destinationPath", path)
      val port = Random.nextInt(Constants.ONE_HUNDRED) + Constants.NINE_THOUSAND
      // Output
      val writeOptions = DataFrameWriterOptions(saveMode = "append",
        options = Some(Map("checkpointLocation" -> checkpointLocation)))
      // Setup the server
      val server = new ServerSocket(port)
      val dataFrame = getReadStream(port, None, None)
      // Start the query
      val query = Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, path).start())
      // Write Data
      val socket = sendRecords(server, Constants.TEN)
      val monitor = Some("com.acxiom.pipeline.streaming.BatchFileStreamingQueryMonitor")
      // Thread the step
      val future = Future {
        FlowUtilsSteps.monitorStreamingQuery(query, monitor, ctx)
      }
      val f = Await.ready(future, Duration.Inf)
      assert(f.isCompleted)
      socket.close()
      server.close()
      // Verify that anything within the path is a file unless it is the _spark_metadata directory
      fs.listStatus(new org.apache.hadoop.fs.Path(path)).foreach(status => {
        assert(!status.isDirectory || status.getPath.toString == s"$path/_spark_metadata")
      })
      val response = f.value.get.get
      assert(response.primaryReturn.getOrElse("continue") == "continue")
      assert(response.namedReturns.isDefined)
      assert(response.namedReturns.get.contains("$globals.destinationPath"))
      val updatedPath = response.namedReturns.get("$globals.destinationPath").asInstanceOf[String]
      assert(updatedPath.substring(updatedPath.lastIndexOf("/") + 1).length == 43)
      assert(updatedPath.substring(updatedPath.lastIndexOf("/") + 1).startsWith("socket_file_"))
      assert(updatedPath.substring(updatedPath.lastIndexOf("/") + 1).endsWith(".parquet"))
      assert(fs.exists(new org.apache.hadoop.fs.Path(checkpointLocation)))
      val hdfs = HDFSDataConnector("TestConnector", None, None)
      val readOptions = DataFrameReaderOptions(schema = Some(Schema.fromStructType(dataFrame.schema)))
      val df = hdfs.load(Some(path), ctx, readOptions)
      assert(df.count() == Constants.TEN)
    }
  }

  private def getReadStream(port: Int, partitionColumn: Option[String], paritionValue: Option[String] = None): DataFrame = {
    val df = sparkSession.readStream.format("socket")
      .option("host", "127.0.0.1").option("port", port).load()
    if (partitionColumn.isDefined) {
      df.withColumn(partitionColumn.get, lit(paritionValue.getOrElse("zero")))
    } else {
      df
    }
  }

  private def sendRecords(server: ServerSocket, count: Int): Socket = {
    val socket = server.accept()
    val socketStream = socket.getOutputStream
    val output = new OutputStreamWriter(socketStream)
    (1 to count).toList.foreach(count => {
      output.write(s"record$count\n")
    })
    output.flush()
    output.close()
    socket
  }
}
