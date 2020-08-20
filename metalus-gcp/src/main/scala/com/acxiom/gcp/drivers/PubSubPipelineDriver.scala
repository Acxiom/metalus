package com.acxiom.gcp.drivers

import com.acxiom.gcp.utils.GCPCredential
import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils, StreamingUtils}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials, SparkPubsubMessage}

object PubSubPipelineDriver {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass", "projectId", "subscription")))
    val commonParameters = DriverUtils.parseCommonParameters(parameters)
    val driverSetup = ReflectionUtils.loadClass(commonParameters.initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    val projectId = parameters("projectId").asInstanceOf[String]
    val subscription = parameters("subscription").asInstanceOf[String]
    logger.info(s"Listening for Pub/Sub messages using project: $projectId")
    logger.info(s"Listening for Pub/Sub messages using subscription: $subscription")
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(
        s"Unable to obtain valid execution plan. Please check the DriverSetup class: ${commonParameters.initializationClass}")
    }
    val executionPlan = driverSetup.executionPlan.get
    val sparkSession = executionPlan.head.pipelineContext.sparkSession.get
    val streamingContext = StreamingUtils.createStreamingContext(sparkSession.sparkContext,
      Some(parameters.getOrElse("duration-type", "seconds").asInstanceOf[String]),
      Some(parameters.getOrElse("duration", "10").asInstanceOf[String]))
    // Get the credential provider
    val credentialProvider = driverSetup.credentialProvider
    val gcpCredential = credentialProvider.getNamedCredential("GCPCredential")
    val sparkGCPCredentials = if (gcpCredential.isDefined) {
      SparkGCPCredentials.builder.jsonServiceAccount(gcpCredential.get.asInstanceOf[GCPCredential].authKey).build()
    } else {
      SparkGCPCredentials.builder.build()
    }
    // Create stream
    val messagesStream = PubsubUtils.createStream(
      streamingContext,
      projectId,
      None,
      subscription, // Cloud Pub/Sub subscription for incoming data
      sparkGCPCredentials,
      StorageLevel.MEMORY_AND_DISK_SER_2)
    val defaultParser = new PubSubStreamingDataParser(subscription)
    val streamingParsers = StreamingUtils.generateStreamingDataParsers(parameters, Some(List(defaultParser)))
    messagesStream.foreachRDD { rdd: RDD[SparkPubsubMessage] =>
      if (!rdd.isEmpty()) {
        logger.debug("RDD received")
        val parser = StreamingUtils.getStreamingParser[SparkPubsubMessage](rdd, streamingParsers)
        val dataFrame = parser.getOrElse(defaultParser).parseRDD(rdd, sparkSession)
        DriverUtils.processExecutionPlan(driverSetup, executionPlan, Some(dataFrame), () => {logger.debug("Completing RDD")},
          commonParameters.terminateAfterFailures, 1, commonParameters.maxRetryAttempts)
      }
    }
    streamingContext.start()
    StreamingUtils.setTerminationState(streamingContext, parameters)
    logger.info("Shutting down GCP PubSub Pipeline Driver")
  }
}
