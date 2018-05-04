package com.acxiom.pipeline.utils

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object StreamingUtils {
  private val logger = Logger.getLogger(getClass)
  private val DEFAULT_DURATION_TYPE = "seconds"
  private val DEFAULT_DURATION = "10"

  /**
    * This function will take the provided SparkContext and optional duration parameters and return a StreamingContext.
    *
    * @param sparkContext The SparkContext to use when creating the streaming context.
    * @param durationType The type of duration, minutes or seconds.
    * @param duration The length of the duration.
    * @return A StreamingContext
    */
  def createStreamingContext(sparkContext: SparkContext,
                             durationType: Option[String] = Some(DEFAULT_DURATION_TYPE),
                             duration: Option[String] = Some(DEFAULT_DURATION)): StreamingContext = {
    new StreamingContext(sparkContext, getDuration(durationType, duration))
  }

  /**
    * This function will take the provided SparkContext and optional duration parameters and return a StreamingContext.
    *
    * @param sparkContext The SparkContext to use when creating the streaming context.
    * @param duration The length of the duration.
    * @return A StreamingContext
    */
  def createStreamingContext(sparkContext: SparkContext,
                             duration: Option[Duration]): StreamingContext = {
    new StreamingContext(sparkContext, duration.getOrElse(getDuration()))
  }

  def getDuration(durationType: Option[String] = Some(DEFAULT_DURATION_TYPE),
                          duration: Option[String] = Some(DEFAULT_DURATION)): Duration = {
    durationType match {
      case Some("seconds") => Seconds(duration.get.toInt)
      case _ => Seconds("30".toInt)
    }
  }

  def setTerminationState(streamingContext: StreamingContext, parameters: Map[String, Any]):Unit = {
    if (parameters.contains("terminationPeriod")) { // This is really just used for testing
      logger.info(s"Streaming Pipeline Driver will terminate after ${parameters("terminationPeriod").asInstanceOf[String]} ms")
      val terminated = streamingContext.awaitTerminationOrTimeout(parameters("terminationPeriod").asInstanceOf[String].toLong)
      if (!terminated) {
        streamingContext.stop(false, true)
      }
    } else {
      logger.info("Streaming Pipeline Driver will wait until process is killed")
      streamingContext.awaitTermination()
    }
  }
}
