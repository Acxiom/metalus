package org.apache.spark.deploy

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

object MetalusSparkHadoopUtil {
  def newConfiguration(conf: SparkConf): Configuration = SparkHadoopUtil.get.newConfiguration(conf)
}
