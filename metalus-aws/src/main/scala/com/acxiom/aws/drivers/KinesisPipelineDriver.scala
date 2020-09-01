package com.acxiom.aws.drivers

import java.util.Date

import com.acxiom.aws.utils.{AWSCredential, KinesisUtilities}
import com.acxiom.pipeline.CredentialProvider
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
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass", "streamName", "region")))
    val commonParameters = DriverUtils.parseCommonParameters(parameters)
    val driverSetup = ReflectionUtils.loadClass(commonParameters.initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(
        s"Unable to obtain valid execution plan. Please check the DriverSetup class: ${commonParameters.initializationClass}")
    }
    val executionPlan = driverSetup.executionPlan.get
    val sparkSession = executionPlan.head.pipelineContext.sparkSession.get
    val region = parameters("region").asInstanceOf[String]
    val streamName = parameters("streamName").asInstanceOf[String]
    val appName = parameters.getOrElse("appName", s"Metalus_Kinesis_$streamName(${new Date().getTime})").asInstanceOf[String]
    // Get the credential provider
    val credentialProvider = driverSetup.credentialProvider
    val awsCredential = credentialProvider.getNamedCredential("AWSCredential").asInstanceOf[Option[AWSCredential]]
    val duration = StreamingUtils.getDuration(Some(parameters.getOrElse("duration-type", "seconds").asInstanceOf[String]),
      Some(parameters.getOrElse("duration", "10").asInstanceOf[String]))
    val streamingContext =
      StreamingUtils.createStreamingContext(sparkSession.sparkContext, Some(duration))
    // Get the client
    val kinesisClient = KinesisUtilities.buildKinesisClient(region, awsCredential)
    // Handle multiple shards
    val numShards = kinesisClient.describeStream(parameters("streamName").asInstanceOf[String]).getStreamDescription.getShards.size
    logger.info("Number of Kinesis shards is : " + numShards)
    val numStreams = parameters.getOrElse("consumerStreams", numShards).asInstanceOf[Int]
    // Create the Kinesis DStreams
    val kinesisStreams = createKinesisDStreams(credentialProvider, appName, duration, streamingContext, numStreams, region, streamName)
    logger.info("Created " + kinesisStreams.size + " Kinesis DStreams")
    val defaultParser = new KinesisStreamingDataParser
    val streamingParsers = StreamingUtils.generateStreamingDataParsers(parameters, Some(List(defaultParser)))
    // Union all the streams (in case numStreams > 1)
    val allStreams = streamingContext.union(kinesisStreams)
    allStreams.foreachRDD { (rdd: RDD[Row], time: Time) =>
      logger.debug(s"Checking RDD for data(${time.toString()}): ${rdd.count()}")
      if (!rdd.isEmpty()) {
        logger.debug("RDD received")
        // Convert the RDD into a dataFrame
        val parser = StreamingUtils.getStreamingParser[Row](rdd, streamingParsers)
        val dataFrame = parser.getOrElse(defaultParser).parseRDD(rdd, sparkSession)
        // Refresh the execution plan prior to processing new data
        DriverUtils.processExecutionPlan(driverSetup, executionPlan, Some(dataFrame), () => {logger.debug("Completing RDD")},
          commonParameters.terminateAfterFailures, 1, commonParameters.maxRetryAttempts)
      }
    }

    streamingContext.start()
    StreamingUtils.setTerminationState(streamingContext, parameters)
    logger.info("Shutting down Kinesis Pipeline Driver")
  }

  private def createKinesisDStreams(credentialProvider: CredentialProvider, appName: String, duration: Duration,
                                    streamingContext: StreamingContext, numStreams: Int, region: String, streamName: String) = {
    val awsCredential = credentialProvider.getNamedCredential("AWSCredential").asInstanceOf[Option[AWSCredential]]
    val cloudWatchCredential = credentialProvider.getNamedCredential("AWSCloudWatchCredential").asInstanceOf[Option[AWSCredential]]
    val dynamoDBCredential = credentialProvider.getNamedCredential("AWSDynamoDBCredential").asInstanceOf[Option[AWSCredential]]
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

      val cloudWatchBuilder = if (cloudWatchCredential.isDefined) {
        builder.cloudWatchCredentials(SparkAWSCredentials.builder.basicCredentials(
            cloudWatchCredential.get.awsAccessKey.get, cloudWatchCredential.get.awsAccessSecret.get).build())
      } else {
        builder
      }

      val dynamoDBBuilder = if (dynamoDBCredential.isDefined) {
        cloudWatchBuilder.dynamoDBCredentials(SparkAWSCredentials.builder.basicCredentials(
            cloudWatchCredential.get.awsAccessKey.get, cloudWatchCredential.get.awsAccessSecret.get).build())
      } else {
        cloudWatchBuilder
      }

      val kinesisBuilder = if (awsCredential.isDefined) {
        dynamoDBBuilder.kinesisCredentials(SparkAWSCredentials.builder.basicCredentials(
            awsCredential.get.awsAccessKey.get, awsCredential.get.awsAccessSecret.get).build())
      } else {
        dynamoDBBuilder
      }

      kinesisBuilder.buildWithMessageHandler((r: Record) => {
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
