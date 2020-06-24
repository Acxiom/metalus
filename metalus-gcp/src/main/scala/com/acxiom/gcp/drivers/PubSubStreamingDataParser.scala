package com.acxiom.gcp.drivers

import java.nio.charset.StandardCharsets

import com.acxiom.pipeline.drivers.StreamingDataParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

class PubSubStreamingDataParser(subscription: String) extends StreamingDataParser[SparkPubsubMessage] {
  /**
    * Responsible for parsing the RDD into a DataFrame. This function will take the value and create
    * a DataFrame with a given structure.
    *
    * @param rdd The RDD to parse
    * @return A DataFrame containing the data in a proper structure.
    */
  override def parseRDD(rdd: RDD[SparkPubsubMessage], sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(rdd.map(message =>
      Row(message.getMessageId(),
        new String(message.getData(), StandardCharsets.UTF_8),
        subscription)),
      StructType(List(StructField("key", StringType),
        StructField("value", StringType),
        StructField("topic", StringType)))).toDF()
  }
}
