package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.PipelineDependencyExecutor
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils, StreamingUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010._

/**
  * Provides a driver that listens to a kafka cluster and one or more topics.
  *
  * Required parameters:
  *
  * "driverSetupClass" - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
  * "topics" - a comma separated list of topics to monitor
  * "kafkaNodes" - a comma separated list of Kafka brokers to consume data
  *
  * Optional Parameters:
  *
  * "duration-type" - should be seconds or minutes
  * "duration" - should be a number
  * "groupId" - should be a string
  * "terminationPeriod" - This is a number (ms) that informs the system to run for the specified amount of time and then shut down.
  */
object KafkaPipelineDriver {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass", "topics", "kafkaNodes")))
    val initializationClass = parameters("driverSetupClass").asInstanceOf[String]
    val driverSetup = ReflectionUtils.loadClass(initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    val topics = parameters("topics").asInstanceOf[String].split(",")
    logger.info(s"Listening for Kafka messages using topics: ${topics.mkString(",")}")
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid execution plan. Please check the DriverSetup class: $initializationClass")
    }
    val executionPlan = driverSetup.executionPlan.get
    val sparkSession = executionPlan.head.pipelineContext.sparkSession.get

    val streamingContext = StreamingUtils.createStreamingContext(sparkSession.sparkContext,
      Some(parameters.getOrElse("duration-type", "seconds").asInstanceOf[String]),
      Some(parameters.getOrElse("duration", "10").asInstanceOf[String]))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> parameters("kafkaNodes").asInstanceOf[String],
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> parameters.getOrElse("groupId", "default_stream_listener").asInstanceOf[String],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd: RDD[ConsumerRecord[String, String]] =>
      if (!rdd.isEmpty()) {
        logger.debug("RDD received")
        // Need to commit the offsets in Kafka that we have consumed
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // Convert the RDD into a dataFrame
        val dataFrame = sparkSession.createDataFrame(rdd.map(r => Row(r.key(), r.value(), r.topic())),
          StructType(List(StructField("key", StringType),
            StructField("value", StringType),
            StructField("topic", StringType)))).toDF()
        // Refresh the execution plan prior to processing new data
        PipelineDependencyExecutor.executePlan(DriverUtils.addInitialDataFrameToExecutionPlan(driverSetup.refreshExecutionPlan(executionPlan), dataFrame))
        // commit offsets after pipeline(s) completes
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        logger.debug(s"Committing Kafka offsets ${offsetRanges.mkString(",")}")
      }
    }

    streamingContext.start()
    StreamingUtils.setTerminationState(streamingContext, parameters)
    logger.info("Shutting down Kafka Pipeline Driver")
  }
}
