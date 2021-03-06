package com.acxiom.pipeline.utils

import com.acxiom.pipeline.drivers.StreamingDataParser
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

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

  def getDuration(durationType: Option[String] = None,
                          duration: Option[String] = None): Duration = {
    val finalDuration = duration.getOrElse(DEFAULT_DURATION).toInt
    durationType.getOrElse(DEFAULT_DURATION_TYPE).toLowerCase match {
      case "milliseconds" => Milliseconds(finalDuration)
      case "seconds" => Seconds(finalDuration)
      case "minutes" => Minutes(finalDuration)
      case _ => Seconds(finalDuration)
    }
  }

  def setTerminationState(streamingContext: StreamingContext, parameters: Map[String, Any]): Unit = {
    val commonParameters = DriverUtils.parseCommonParameters(parameters)
    try {
      if (parameters.contains("terminationPeriod")) { // This is really just used for testing
        logger.info(s"Streaming Pipeline Driver will terminate after ${parameters("terminationPeriod").asInstanceOf[String]} ms")
        val terminated = streamingContext.awaitTerminationOrTimeout(parameters("terminationPeriod").asInstanceOf[String].toLong)
        if (!terminated) {
          streamingContext.stop(stopSparkContext = false, stopGracefully = true)
        }
      } else {
        logger.info("Streaming Pipeline Driver will wait until process is killed")
        streamingContext.awaitTermination()
      }
    } catch {
      case t: Throwable if commonParameters.terminateAfterFailures =>
        streamingContext.stop(stopSparkContext = true, stopGracefully = true)
        throw t
      case t: Throwable => throw t
    }
  }

  /**
    * Helper function to parse and initialize the StreamingParsers from the command line.
    * @param parameters The input parameters
    * @param parsers An initial list of parsers. The new parsers will be prepended to this list.
    * @return A list of streaming parsers
    */
  def generateStreamingDataParsers[T](parameters: Map[String, Any],
                                      parsers: Option[List[StreamingDataParser[T]]] = None): List[StreamingDataParser[T]] = {
    val parsersList = if (parsers.isDefined) {
      parsers.get
    } else {
      List[StreamingDataParser[T]]()
    }
    // Add any parsers to the head of the list
    if (parameters.contains("streaming-parsers")) {
      parameters("streaming-parsers").asInstanceOf[String].split(',').foldLeft(parsersList)((list, p) => {
        ReflectionUtils.loadClass(p, Some(parameters)).asInstanceOf[StreamingDataParser[T]] :: list
      })
    } else {
      parsersList
    }
  }

  /**
    * Helper function that will attempt to find the appropriate parse for the provided RDD.
    * @param rdd The RDD to parse.
    * @param parsers A list of parsers tp consider.
    * @return The first parser that indicates it can parse the RDD.
    */
  def getStreamingParser[T](rdd: RDD[T], parsers: List[StreamingDataParser[T]]): Option[StreamingDataParser[T]] =
    parsers.find(p => p.canParse(rdd))

  /**
   * Helper function that build a CommonStreamingParameters object using the parameters map.
   * @param parameters passed to metalus from the spark submit.
   * @return a CommonStreamingParameters object
   */
  def parseCommonStreamingParameters(parameters: Map[String, Any]): CommonStreamingParameters = {
    CommonStreamingParameters(parameters.getOrElse("processEmptyRDD", false).asInstanceOf[Boolean])
  }
}

case class CommonStreamingParameters(processEmptyRDD: Boolean)
