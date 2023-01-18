package com.acxiom.metalus.utils

//import org.apache.spark.streaming._

// TODO [2.0 Review] Move to spark project
object StreamingUtils {
  private val DEFAULT_DURATION_TYPE = "seconds"
  private val DEFAULT_DURATION = "10"

//  def getDuration(durationType: Option[String] = None,
//                          duration: Option[String] = None): Duration = {
//    val finalDuration = duration.getOrElse(DEFAULT_DURATION).toInt
//    durationType.getOrElse(DEFAULT_DURATION_TYPE).toLowerCase match {
//      case "milliseconds" => Milliseconds(finalDuration)
//      case "seconds" => Seconds(finalDuration)
//      case "minutes" => Minutes(finalDuration)
//      case _ => Seconds(finalDuration)
//    }
//  }
}
