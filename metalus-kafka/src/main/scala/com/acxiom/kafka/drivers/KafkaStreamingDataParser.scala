package com.acxiom.kafka.drivers

import com.acxiom.pipeline.drivers.StreamingDataParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class KafkaStreamingDataParser extends StreamingDataParser[ConsumerRecord[String, String], Row] {
  /**
    * Responsible for parsing the RDD into a DataFrame. This function will take the value and create
    * a DataFrame with a given structure.
    *
    * @param rdd The RDD to parse
    * @return A DataFrame containing the data in a proper structure.
    */
  override def parseRDD(rdd: RDD[ConsumerRecord[String, String]], sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(rdd.map(r => Row(r.key(), r.value(), r.topic())),
      StructType(List(StructField("key", StringType),
        StructField("value", StringType),
        StructField("topic", StringType)))).toDF()
  }

  /**
    * Determines if this parser can parse the incoming data
    *
    * @param rdd The rdd to parse
    * @return true if this parser can parse the incoming data
    */
  override def canParse(rdd: RDD[ConsumerRecord[String, String]]): Boolean = true
}
