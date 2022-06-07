package com.acxiom.aws.pipeline.connectors

import com.acxiom.aws.utils.{AWSCredential, KinesisUtilities}
import com.acxiom.pipeline.connectors.ConnectorWriter
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import org.apache.spark.sql.{ForeachWriter, Row}

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

/**
  * Write a batch DataFrame to Kinesis using record batching. The following parameters are expected:
  * streamName The Kinesis stream name
  * region The region of the Kinesis stream
  * dataFrame The DataFrame to write
  * partitionKey The static partition key to use
  * partitionKeyIndex The field index in the DataFrame row containing the value to use as the partition key
  * separator The field separator to use when formatting the row data
  * credential An optional credential to use to authenticate to Kinesis
  */
trait KinesisWriter extends ConnectorWriter {
  // Kinesis Client Limits
  val maxBufferSize: Int = 500 * 1024
  val maxRecords = 500
  // Buffers
  protected val buffer = new ArrayBuffer[PutRecordsRequestEntry]()
  protected var bufferSize: Long = 0L
  // Client library
  protected var kinesisClient: AmazonKinesis = _

  def streamName: String
  def region: String
  def credential: Option[AWSCredential]
  def partitionKey: Option[String]
  def partitionKeyIndex: Option[Int]
  def separator: String

  private lazy val defaultPartitionKey = java.util.UUID.randomUUID().toString

  def open(): Unit = {
    kinesisClient = KinesisUtilities.buildKinesisClient(region, credential)
  }

  def close(): Unit = {
    if (buffer.nonEmpty) {
      flush()
    }
    kinesisClient.shutdown()
  }

  def process(value: Row): Unit = {
    val data = value.mkString(separator).getBytes
    if ((data.length + bufferSize > maxBufferSize && buffer.nonEmpty) || buffer.length == maxRecords) {
      flush()
    }
    val putRecordRequest = new PutRecordsRequestEntry()
    buffer += putRecordRequest.withPartitionKey(getPartitionKey(value)).withData(ByteBuffer.wrap(data))
    bufferSize += data.length
  }

  private def getPartitionKey(value: Row): String = partitionKey
      .orElse(partitionKeyIndex.map(i => value.get(i).toString))
      .filter(s => Option(s).nonEmpty && s.nonEmpty)
      .getOrElse(defaultPartitionKey)

  private def flush(): Unit = {
    val recordRequest = new PutRecordsRequest()
      .withStreamName(streamName)
      .withRecords(buffer: _*)

    kinesisClient.putRecords(recordRequest)
    buffer.clear()
    bufferSize = 0
  }
}

class BatchKinesisWriter(override val streamName: String,
                         override val region: String,
                         override val partitionKey: Option[String],
                         override val partitionKeyIndex: Option[Int],
                         override val separator: String,
                         override val credential: Option[AWSCredential]) extends KinesisWriter

class StructuredStreamingKinesisSink(override val streamName: String,
                                     override val region: String,
                                     override val partitionKey: Option[String],
                                     override val partitionKeyIndex: Option[Int],
                                     override val separator: String,
                                     override val credential: Option[AWSCredential]) extends ForeachWriter[Row] with KinesisWriter {
  override def open(partitionId: Long, epochId: Long): Boolean = {
    this.open()
    true
  }

  override def process(value: Row): Unit = super.process(_: Row)

  override def close(errorOrNull: Throwable): Unit = this.close()
}
