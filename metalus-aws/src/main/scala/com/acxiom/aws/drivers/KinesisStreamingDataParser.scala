package com.acxiom.aws.drivers

import com.acxiom.pipeline.drivers.StreamingDataParser
import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class KinesisStreamingDataParser(appName: String) extends StreamingDataParser[Record] {
  /**
    * Responsible for parsing the RDD into a DataFrame. This function will take the value and create
    * a DataFrame with a given structure.
    *
    * @param rdd The RDD to parse
    * @return A DataFrame containing the data in a proper structure.
    */
  override def parseRDD(rdd: RDD[Record], sparkSession: SparkSession): DataFrame = {
    sparkSession.createDataFrame(rdd.map(r => Row(r.getPartitionKey, r.getData, appName)),
      StructType(List(StructField("key", StringType), StructField("value", StringType),
        StructField("topic", StringType)))).toDF()
  }
}
