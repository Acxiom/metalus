package com.acxiom.aws.drivers

import java.util.Date

import com.acxiom.aws.utils.KinesisUtilities
import com.acxiom.pipeline.PipelineDependencyExecutor
import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils, StreamingUtils}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream, SparkAWSCredentials}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

/**
  * Provides a driver that listens to a kafka cluster and one or more topics.
  *
  * Required parameters:
  *
  * "driverSetupClass" - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
  * "streamName" - The Kinesis stream where data will be pulled
  * "region" - A valid AWS region
  *
  * Optional Parameters:
  *
  * "appName" - The name of this app to use when check pointing Kinesis sequence numbers.
  * "awsAccessKey" - The AWS access key used to connect
  * "awsAccessSecret" - The AWS access secret used to connect
  * "duration-type" - should be seconds or minutes
  * "duration" - should be a number
  * "terminationPeriod" - This is a number (ms) that informs the system to run for the specified amount of time and then shut down.
  */
object KinesisPipelineDriver {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args,
      Some(List("driverSetupClass", "streamName", "region")))
    val initializationClass = parameters("driverSetupClass").asInstanceOf[String]
    val driverSetup = ReflectionUtils.loadClass(initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid execution plan. Please check the DriverSetup class: $initializationClass")
    }
    val executionPlan = driverSetup.executionPlan.get
    val sparkSession = executionPlan.head.pipelineContext.sparkSession.get
    val region = parameters("region").asInstanceOf[String]
    val streamName = parameters("streamName").asInstanceOf[String]
    val awsAccessKey = parameters.get("accessKeyId").asInstanceOf[Option[String]]
    val awsAccessSecret = parameters.get("secretAccessKey").asInstanceOf[Option[String]]
    val appName = parameters.getOrElse("appName", s"Metalus_Kinesis_$streamName(${new Date().getTime})").asInstanceOf[String]

    val duration = StreamingUtils.getDuration(Some(parameters.getOrElse("duration-type", "seconds").asInstanceOf[String]),
      Some(parameters.getOrElse("duration", "10").asInstanceOf[String]))
    val streamingContext =
      StreamingUtils.createStreamingContext(sparkSession.sparkContext, Some(duration))
    // Get the client
    val kinesisClient = KinesisUtilities.buildKinesisClient(region, awsAccessKey, awsAccessSecret)
    // Handle multiple shards
    val numShards = kinesisClient.describeStream(parameters("streamName").asInstanceOf[String]).getStreamDescription.getShards.size
    logger.info("Number of Kinesis shards is : " + numShards)
    val numStreams = parameters.getOrElse("consumerStreams", numShards).asInstanceOf[Int]
    // Create the Kinesis DStreams
    val kinesisStreams = createKinesisDStreams(awsAccessKey, awsAccessSecret, appName, duration, streamingContext, numStreams, region, streamName)
    logger.info("Created " + kinesisStreams.size + " Kinesis DStreams")
    val defaultParser = new KinesisStreamingDataParser
    val streamingParsers = DriverUtils.generateStreamingDataParsers(parameters, Some(List(defaultParser)))
    // Union all the streams (in case numStreams > 1)
    val allStreams = streamingContext.union(kinesisStreams)
    allStreams.foreachRDD { (rdd: RDD[Row], time: Time) =>
      logger.debug(s"Checking RDD for data(${time.toString()}): ${rdd.count()}")
      if (!rdd.isEmpty()) {
        logger.debug("RDD received")
        // Convert the RDD into a dataFrame
        val parser = DriverUtils.getStreamingParser[Row](rdd, streamingParsers)
        val dataFrame = parser.getOrElse(defaultParser).parseRDD(rdd, sparkSession)
        // Refresh the execution plan prior to processing new data
        PipelineDependencyExecutor.executePlan(DriverUtils.addInitialDataFrameToExecutionPlan(driverSetup.refreshExecutionPlan(executionPlan), dataFrame))
        logger.debug("Completing RDD")
      }
    }

    streamingContext.start()
    StreamingUtils.setTerminationState(streamingContext, parameters)
    logger.info("Shutting down Kinesis Pipeline Driver")
  }

  private def createKinesisDStreams(awsAccessKey: Option[String], awsAccessSecret: Option[String], appName: String, duration: Duration,
                                    streamingContext: StreamingContext, numStreams: Int, region: String, streamName: String) = {
    (0 until numStreams).map { _ =>
      val builder = KinesisInputDStream.builder
        .endpointUrl(s"https://kinesis.$region.amazonaws.com")
        .streamName(streamName)
        .regionName(region)
        .streamingContext(streamingContext)
        .checkpointAppName(appName)
        .checkpointInterval(duration)
        .initialPosition(KinesisInitialPositions.fromKinesisInitialPosition(InitialPositionInStream.LATEST))
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)

      (if (awsAccessKey.isDefined) {
        builder.kinesisCredentials(SparkAWSCredentials.builder.basicCredentials(awsAccessKey.get, awsAccessSecret.get).build())
      } else {
        builder
      }).buildWithMessageHandler((r: Record) => {
        try {
          Row(r.getPartitionKey, new String(r.getData.array()), appName)
        } catch {
          case t: Throwable =>
            logger.error(s"Unable to parse incoming record: ${t.getMessage}", t)
            throw t
        }
      })
    }
  }
}
