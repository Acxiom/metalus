package com.acxiom.pipeline.utils

import org.apache.spark.streaming._

object StreamingUtils {
  private val DEFAULT_DURATION_TYPE = "seconds"
  private val DEFAULT_DURATION = "10"

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
}
