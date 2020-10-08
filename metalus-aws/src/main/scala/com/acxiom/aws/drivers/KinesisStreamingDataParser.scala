package com.acxiom.aws.drivers

import com.acxiom.pipeline.drivers.StreamingDataParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class KinesisStreamingDataParser extends StreamingDataParser[Row] {
  /**
    * Responsible for parsing the RDD into a DataFrame. This function will take the value and create
    * a DataFrame with a given structure.
    *
    * @param rdd The RDD to parse
    * @return A DataFrame containing the data in a proper structure.
    */
  override def parseRDD(rdd: RDD[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(rdd,
      StructType(List(StructField("key", StringType), StructField("value", StringType),
        StructField("topic", StringType)))).toDF()
  }
}
