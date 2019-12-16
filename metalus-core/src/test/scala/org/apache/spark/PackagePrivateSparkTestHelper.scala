package org.apache.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerInterface}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object PackagePrivateSparkTestHelper {

  def getSparkListeners(spark: SparkSession): List[SparkListenerInterface] ={
    spark.sparkContext.listenerBus.listeners.asScala.toList
  }

}
