package com.acxiom.metalus.aws.drivers

import com.acxiom.metalus._
import com.acxiom.metalus.aws.utils.{AWSCredential, AWSUtilities}
import com.acxiom.metalus.drivers.{DriverSetup, StreamingDataParser}
import com.acxiom.metalus.utils.DriverUtils.buildPipelineException
import com.acxiom.metalus.utils.{DriverUtils, ReflectionUtils}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.kinesis.model.{GetRecordsRequest, GetShardIteratorRequest, ProvisionedThroughputExceededException, Record}
import software.amazon.awssdk.services.kinesis.{KinesisClient, KinesisClientBuilder}

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/*
 *  Example of reading data from a shard (this would happen in each shard thread)
 *  https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/javav2/example_code/kinesis/src/main/java/com/example/kinesis/GetRecords.java
 * GetRecords API docs:
 *  https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/kinesis/KinesisClient.html#getRecords(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest)
 * Documentation should highlight that multiple shards can be monitored with a single consumer, but for high throughput
 *    it is recommended that multiple consumers are used.
 */
object KinesisDriver {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, None)
    val commonParameters = DriverUtils.parseCommonParameters(parameters)
    // Validate Kinesis parameters
    if (!parameters.contains("shardIds")) {
      throw new IllegalArgumentException("Missing required parameters: shardIds")
    }
    if (!parameters.contains("streamName")) {
      throw new IllegalArgumentException("Missing required parameters: streamName")
    }
    val defaultParser = KinesisStreamingDataParser(parameters.getOrElse("kinesisRecordFormat", "csv").toString,
      parameters.getOrElse("kinesisRecordSeparator", ",").toString)
    val parsers = DriverUtils.generateStreamingDataParsers[Record](parameters, Some(List(defaultParser)))
    val batchSize = parameters.getOrElse("streamingBatchSize", "100").toString.toInt

    // Spin up the consumer threads
    val futures = parameters("shardIds").toString.split(",")
      .map(shardId => {
        val driverSetup = ReflectionUtils.loadClass(commonParameters.initializationClass,
          Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
        if (driverSetup.pipeline.isEmpty) {
          throw new IllegalStateException(s"Unable to obtain valid pipeline. Please check the DriverSetup class: ${commonParameters.initializationClass}")
        }
        val pipelineContext = driverSetup.pipelineContext
        val credential = if (parameters.contains("streamingCredential")) {
          pipelineContext.credentialProvider.get
            .getNamedCredential(parameters("streamingCredential").toString).asInstanceOf[Option[AWSCredential]]
        } else {
          None
        }
        val kinesisClient = AWSUtilities.setupCredentialProvider(KinesisClient.builder(), credential)
          .asInstanceOf[KinesisClientBuilder].build()
        consumeShard(kinesisClient, parsers, parameters("streamName").toString,
          shardId, batchSize, parameters.getOrElse("streamStartingPosition", "LATEST").toString,
          driverSetup.pipelineContext, driverSetup.pipeline.get)
      }).toSeq
    Await.ready(Future.sequence(futures), Duration.Inf)
  }

  private def consumeShard(kinesisClient: KinesisClient,
                           parsers: List[StreamingDataParser[Record]],
                           streamName: String,
                           shardId: String,
                           batchSize: Int = Constants.ONE_THOUSAND,
                           streamStartingPosition: String = "LATEST",
                           pipelineContext: PipelineContext,
                           pipeline: Pipeline): Future[Boolean] = {
    val itReq = GetShardIteratorRequest.builder()
      .streamName(streamName)
      .shardIteratorType(streamStartingPosition)
      .shardId(shardId)
      .build()
    val shardIteratorResult = kinesisClient.getShardIterator(itReq)
    val shardIterator = shardIteratorResult.shardIterator
    val recordsRequest = GetRecordsRequest.builder()
      .shardIterator(shardIterator)
      .limit(batchSize)
      .build()
    implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    Future {
      pollShard(kinesisClient, parsers, recordsRequest, pipelineContext, pipeline)
    }
  }

  @tailrec
  private def pollShard(kinesisClient: KinesisClient,
                        parsers: List[StreamingDataParser[Record]],
                        recordsRequest: GetRecordsRequest,
                        pipelineContext: PipelineContext,
                        pipeline: Pipeline,
                        retryPolicy: RetryPolicy = RetryPolicy(),
                        retryCount: Int = 0): Boolean = {
    try {
      Iterator.continually(kinesisClient.getRecords(recordsRequest))
        .takeWhile(r => Option(r.nextShardIterator()).isDefined)
        .foreach(result => {
          val records = result.records
          val recordGroups = records.asScala.foldLeft(Map[String, (StreamingDataParser[Record], List[Record])]())((groups, record) => {
            val parser = DriverUtils.getStreamingParser[Record](record, parsers)
            val key = if (parser.isDefined) {
              parser.get.getClass.toString
            } else {
              ""
            }
            val value = groups.getOrElse(key, (parser.get, List()))
            groups + (key -> (parser.get, value._2 :+ record))
          })
          recordGroups.foreach(group => {
            val rows = group._2._1.parseRecords(group._2._2)
            // TODO Use an in-memory DataReference
            val context = pipelineContext.setGlobal("incomingDataReference", rows)
            PipelineExecutor.executePipelines(pipeline, context)
          })
        })
      true
    } catch {
      case r: ProvisionedThroughputExceededException =>
        if (retryCount < retryPolicy.maximumRetries.getOrElse(Constants.TEN)) {
          DriverUtils.invokeWaitPeriod(retryPolicy, retryCount + 1)
          pollShard(kinesisClient, parsers, recordsRequest, pipelineContext, pipeline, retryPolicy, retryCount + 1)
        } else {
          throw buildPipelineException(Some(s"Unable to read records from stream ${recordsRequest.streamARN()}"), Some(r), None)
        }
    }
  }
}
