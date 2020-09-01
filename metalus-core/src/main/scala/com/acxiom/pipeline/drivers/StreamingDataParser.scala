package com.acxiom.pipeline.drivers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

trait StreamingDataParser[T] extends Serializable {
  /**
    * Determines if this parser can parse the incoming data
    *
    * @param rdd The rdd to parse
    * @return true if this parser can parse the incoming data
    */
  def canParse(rdd: RDD[T]): Boolean = true

  /**
    * Responsible for parsing the RDD into a DataFrame. This function will take the value and create
    * a DataFrame with a given structure.
    * @param rdd The RDD to parse
    * @return A DataFrame containing the data in a proper structure.
    */
  def parseRDD(rdd: RDD[T], sparkSession: SparkSession): DataFrame
}
