package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.PipelineDependencyExecutor
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils, StreamingUtils}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Provides a driver that listens to a kafka cluster and one or more topics.
  *
  * Required parameters:
  *
  * "driverSetupClass" - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
  * "appName" - The name of this app to use when check pointing Kinesis sequence numbers.
  * "streamName" - The Kinesis stream where data will be pulled
  * "endPointURL" - The Kinesis URL where to connect the app.
  * "regionName" - A valid AWS region
  * "awsAccessKey" - The AWS access key used to connect
  * "awsAccessSecret" - The AWS access secret used to connect
  *
  * Optional Parameters:
  *
  * "duration-type" - should be seconds or minutes
  * "duration" - should be a number
  * "terminationPeriod" - This is a number (ms) that informs the system to run for the specified amount of time and then shut down.
  */
object KinesisPipelineDriver {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args,
      Some(List("driverSetupClass", "appName", "streamName", "endPointURL", "regionName", "awsAccessKey", "awsAccessSecret")))
    val initializationClass = parameters("driverSetupClass").asInstanceOf[String]
    val driverSetup = ReflectionUtils.loadClass(initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid execution plan. Please check the DriverSetup class: $initializationClass")
    }
    val executionPlan = driverSetup.executionPlan.get
    val sparkSession = executionPlan.head.pipelineContext.sparkSession.get
    val awsAccessKey = parameters("awsAccessKey").asInstanceOf[String]
    val awsAccessSecret = parameters("awsAccessSecret").asInstanceOf[String]
    val appName = parameters("appName").asInstanceOf[String]

    val duration = StreamingUtils.getDuration(Some(parameters.getOrElse("duration-type", "seconds").asInstanceOf[String]),
      Some(parameters.getOrElse("duration", "10").asInstanceOf[String]))
    val streamingContext =
      StreamingUtils.createStreamingContext(sparkSession.sparkContext, Some(duration))

    // Handle multiple shards
    val credentials = new BasicAWSCredentials(awsAccessKey, awsAccessSecret)
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(parameters("endPointURL").asInstanceOf[String])
    val numShards = kinesisClient.describeStream(parameters("streamName").asInstanceOf[String]).getStreamDescription.getShards.size
    logger.info("Number of Kinesis shards is : " + numShards)
    val numStreams = parameters.getOrElse("consumerStreams", numShards).asInstanceOf[Int]

    // Create the Kinesis DStreams
    val kinesisStreams = createKinesisDStreams(awsAccessKey, awsAccessSecret, appName, duration, streamingContext, numStreams, parameters)
    logger.info("Created " + kinesisStreams.size + " Kinesis DStreams")

    // Union all the streams (in case numStreams > 1)
    val allStreams = streamingContext.union(kinesisStreams)
    allStreams.foreachRDD { rdd: RDD[Row] =>
      if (!rdd.isEmpty()) {
        logger.debug("RDD received")
        // Convert the RDD into a dataFrame
        val dataFrame = sparkSession.createDataFrame(rdd,
          StructType(List(StructField("key", StringType), StructField("value", StringType),
            StructField("topic", StringType)))).toDF()
        // Refresh the execution plan prior to processing new data
        PipelineDependencyExecutor.executePlan(DriverUtils.addInitialDataFrameToExecutionPlan(driverSetup.refreshExecutionPlan(executionPlan), dataFrame))
        logger.debug("Completing RDD")
      }
    }

    streamingContext.start()
    StreamingUtils.setTerminationState(streamingContext, parameters)
    logger.info("Shutting down Kinesis Pipeline Driver")
  }

  private def createKinesisDStreams(awsAccessKey: String, awsAccessSecret: String, appName: String, duration: Duration,
                                    streamingContext: StreamingContext, numStreams: Int, parameters: Map[String, Any]) = {
    (0 until numStreams).map { _ =>
      KinesisUtils.createStream(streamingContext, appName,
        parameters("streamName").asInstanceOf[String],
        parameters("endPointURL").asInstanceOf[String],
        parameters("regionName").asInstanceOf[String],
        InitialPositionInStream.LATEST, duration, StorageLevel.MEMORY_AND_DISK_2,
        (r: Record) => Row(r.getPartitionKey, new String(r.getData.array()), appName),
        awsAccessKey, awsAccessSecret)
    }
  }
}
