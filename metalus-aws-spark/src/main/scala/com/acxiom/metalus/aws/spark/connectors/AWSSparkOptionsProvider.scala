package com.acxiom.metalus.aws.spark.connectors

import com.acxiom.metalus.aws.utils.AWSCredential
import com.acxiom.metalus.spark.connectors.SparkOptionsProvider

final class AWSSparkOptionsProvider extends SparkOptionsProvider {
  override def getReadOptions: PartialFunction[Any, Map[String, String]] = {
    case c: AWSCredential => c.toSparkOptions
  }

  override def getWriteOptions: PartialFunction[Any, Map[String, String]] = {
    case c: AWSCredential => c.toSparkOptions
  }
}
