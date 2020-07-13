package com.acxiom.aws.steps

import java.nio.ByteBuffer

import com.acxiom.aws.utils.KinesisUtilities
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.sql.DataFrame

@StepObject
object KinesisSteps {
  @StepFunction("207aa871-4f83-4e24-bab3-4e47bb3b667a",
    "Write DataFrame to a Kinesis Stream",
    "This step will write a DataFrame to a Kinesis Stream",
    "Pipeline",
    "AWS")
  def writeToStream(dataFrame: DataFrame,
                    region: String,
                    streamName: String,
                    partitionKey: String,
                    separator: String = ",",
                    accessKeyId: Option[String] = None,
                    secretAccessKey: Option[String] = None): Unit = {
    val index = if (dataFrame.schema.isEmpty) {
      0
    } else {
      val field = dataFrame.schema.fieldIndex(partitionKey)
      if (field < 0) {
        0
      } else {
        field
      }
    }
    dataFrame.rdd.foreach(row => {
      val rowData = row.mkString(separator)
      val key = row.getAs[Any](index).toString
      val putRecordRequest = new PutRecordRequest()
      putRecordRequest.setStreamName(streamName)
      putRecordRequest.setPartitionKey(key)
      putRecordRequest.setData(ByteBuffer.wrap(String.valueOf(rowData).getBytes()))
      val kinesisClient = KinesisUtilities.buildKinesisClientByKeys(region, accessKeyId, secretAccessKey)
      kinesisClient.putRecord(putRecordRequest)
      kinesisClient.shutdown()
    })
  }
}
