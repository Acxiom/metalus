package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.PipelineExecutor
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

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
  */
class KafkaPipelineDriver {
  private val DEFAULT_DURATION_TYPE = "seconds"
  private val DEFAULT_DURATION = "10"

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass", "topics", "kafkaNodes")))
    val initializationClass = parameters("driverSetupClass").asInstanceOf[String]
    val driverSetup = ReflectionUtils.loadClass(initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]

    val topics = parameters("topics").asInstanceOf[String].split(",")

    val pipelineContext = driverSetup.pipelineContext

    val fieldDelimiter = pipelineContext.globals.get.getOrElse("fieldDelimiter", ",").asInstanceOf[String]

    val streamingContext = new StreamingContext(pipelineContext.sparkConf.get,
      getDuration(Some(parameters.getOrElse("duration-type", "seconds").asInstanceOf[String]),
        Some(parameters.getOrElse("duration", "10").asInstanceOf[String])))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> parameters("kafkaNodes").asInstanceOf[String],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        // Need to commit the offsets in Kafka that we have consumed
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // Convert the RDD into a dataFrame
        val dataFrame = rdd.map(record => Row(record.value.split(fieldDelimiter)))
        // Refresh the pipelineContext prior to run new data
        val ctx = driverSetup.refreshContext(pipelineContext).setGlobal("initialDataFrame", dataFrame)
        // Process the data frame
        PipelineExecutor.executePipelines(driverSetup.pipelines,
          if (driverSetup.initialPipelineId == "") None else Some(driverSetup.initialPipelineId), ctx)
        // commit offsets after pipeline(s) completes
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def getDuration(durationType: Option[String] = Some(DEFAULT_DURATION_TYPE),
                          duration: Option[String] = Some(DEFAULT_DURATION)): Duration = {
    durationType match {
      case Some("seconds") => Seconds(duration.get.toInt)
      case _ => Seconds("30".toInt)
    }
  }
}
