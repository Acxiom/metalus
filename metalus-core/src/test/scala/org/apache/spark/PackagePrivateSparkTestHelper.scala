package org.apache.spark

import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

object PackagePrivateSparkTestHelper {

  def getSparkListeners(spark: SparkSession): List[SparkListenerInterface] ={
    spark.sparkContext.listenerBus.listeners.asScala.toList
  }

}
