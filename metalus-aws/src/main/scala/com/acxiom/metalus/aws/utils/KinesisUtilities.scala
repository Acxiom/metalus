package com.acxiom.metalus.aws.utils

import com.acxiom.metalus.sql.Row
import com.acxiom.metalus.utils.DriverUtils.{buildPipelineException, invokeWaitPeriod}
import com.acxiom.metalus.{Constants, PipelineException, RetryPolicy}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._
import software.amazon.awssdk.services.kinesis.{KinesisClient, KinesisClientBuilder}

import java.net.URI
import java.util
import scala.jdk.CollectionConverters._

object KinesisUtilities {

  /**
   * Write a single message to a Kinesis Stream
   *
   * @param message         The message to post to the Kinesis stream
   * @param region          The region of the Kinesis stream
   * @param streamName      The name of the Kinesis stream
   * @param partitionKey    The key to use when partitioning the message
   * @param accessKeyId     The optional API key to use for the Kinesis stream
   * @param secretAccessKey The optional API secret to use for the Kinesis stream
   */
  def postMessage(message: String,
                  region: String,
                  streamName: String,
                  partitionKey: String,
                  accessKeyId: Option[String] = None,
                  secretAccessKey: Option[String] = None): Unit = {
    val kinesisProducer = KinesisProducer(streamName, region, partitionKey = Some(partitionKey),
      credential = Some(new DefaultAWSCredential(Map("accessKeyId" -> accessKeyId, "secretAccessKey" -> secretAccessKey))))
    kinesisProducer.process(Row(Array(message), None, None))
    kinesisProducer.close()
  }

  /**
   * Write a single message to a Kinesis Stream
   *
   * @param message      The message to post to the Kinesis stream
   * @param region       The region of the Kinesis stream
   * @param streamName   The name of the Kinesis stream
   * @param partitionKey The key to use when partitioning the message
   * @param credential   The optional AWSCredential object use to auth to the Kinesis stream
   */
  def postMessageWithCredentials(message: String,
                                 region: String,
                                 streamName: String,
                                 partitionKey: String,
                                 credential: Option[AWSCredential] = None): Unit = {
    val kinesisProducer = KinesisProducer(streamName, region, partitionKey = Some(partitionKey), credential = credential)
    kinesisProducer.process(Row(Array(message), None, None))
    kinesisProducer.close()
  }

//  /**
//   * Determines the column id to use to extract the partition key value when writing rows
//   *
//   * @param dataFrame    The DataFrame containing the schema
//   * @param partitionKey The field name of the column to use for the key value.
//   * @return The column index or zero id the column name is not found.
//   */
  //  def determinePartitionKey(dataFrame: DataFrame, partitionKey: String): Int = {
  //    if (dataFrame.schema.isEmpty) {
  //      0
  //    } else {
  //      val field = dataFrame.schema.fieldIndex(partitionKey)
  //      if (field < 0) {
  //        0
  //      } else {
  //        field
  //      }
  //    }
  //  }

//  /**
//   * Write a batch DataFrame to Kinesis using record batching.
//   *
//   * @param dataFrame         The DataFrame to write
//   * @param region            The region of the Kinesis stream
//   * @param streamName        The Kinesis stream name
//   * @param partitionKey      The static partition key to use
//   * @param partitionKeyIndex The field index in the DataFrame row containing the value to use as the partition key
//   * @param separator         The field separator to use when formatting the row data
//   * @param credential        An optional credential to use to authenticate to Kinesis
//   */
//  def writeDataFrame(dataFrame: DataFrame,
//                     region: String,
//                     streamName: String,
//                     partitionKey: Option[String],
//                     partitionKeyIndex: Option[Int],
//                     separator: String = ",",
//                     credential: Option[AWSCredential] = None): Unit = {
//    dataFrame.rdd.foreachPartition(rows => {
//      val writer = new BatchKinesisWriter(streamName, region, partitionKey, partitionKeyIndex, separator, credential)
//      writer.open()
//      rows.foreach(writer.process)
//      writer.close()
//    })
//  }
}

case class KinesisProducer(streamName: String,
                           region: String,
                           separator: String = ",",
                           partitionKey: Option[String] = None,
                           partitionKeyIndex: Option[Int] = None,
                           retryPolicy: Option[RetryPolicy] = Some(RetryPolicy()),
                           credential: Option[AWSCredential] = None) {
  private lazy val defaultPartitionKey = java.util.UUID.randomUUID().toString
  private val builder = AWSUtilities.setupCredentialProvider(KinesisClient.builder(), credential).asInstanceOf[KinesisClientBuilder]
  private val kinesisClient = builder.endpointOverride(new URI(s"https://kinesis.$region.amazonaws.com")).build()
  private val MAX_ROW_SIZE = 1048576
  private val MAX_REQUEST_SIZE = Constants.FIVE * MAX_ROW_SIZE

  /**
   * Closes the connection to the Kinesis stream. Once this is called, this producer can no longer be used.
   */
  def close(): Unit = kinesisClient.close()

  /**
   * Prepares the provided row and pushes to the Kinesis stream. The data from the Row will be
   * converted in a string using the separator character.
   *
   * @param row A single row to push to the stream.
   * @throws PipelineException - will be thrown if this call cannot be completed.
   */
  @throws(classOf[PipelineException])
  def process(row: Row): Unit = process(List(row))

  /**
   * Prepares the provided rows and pushes to the Kinesis stream. The data from each Row will be
   * converted in a string using the separator character. This method attempt to retry if throughput
   * is exceeded based on the retryPolicy. Additionally, this method will attempt to ensure that the
   * provided rows are able to be sent unless the data is too large.
   *
   * @param rows A list of Row objects.
   * @throws PipelineException - will be thrown if this call cannot be completed.
   */
  @throws(classOf[PipelineException])
  def process(rows: List[Row]): Unit = {
    val requests = rows.foldLeft((List[util.List[PutRecordsRequestEntry]](), List[PutRecordsRequestEntry](), 0))((result, row) => {
      val data = row.mkString(separator).getBytes
      val currentSize = result._3 + data.length
      if (data.length > MAX_ROW_SIZE) {
        throw buildPipelineException(
          Some(s"Unable to put records to stream $streamName as a result of a row being larger than 1 MiB: ${data.length}"), None, None)
      }
      val entry = PutRecordsRequestEntry.builder()
        .data(SdkBytes.fromByteArray(data))
        .partitionKey(getPartitionKey(row))
        .build()
      if (result._2.length < 500 && currentSize < MAX_REQUEST_SIZE) {
        (result._1, result._2 :+ entry, currentSize)
      } else {
        (result._1 :+ result._2.asJava, List(entry), data.length)
      }
    })
    requests._1.foreach(putRecords(_))
  }

  private def putRecords(requests: util.List[PutRecordsRequestEntry], retryCount: Int = 0): Unit = {
    try {
      kinesisClient.putRecords(PutRecordsRequest.builder().streamName(streamName).records(requests).build())
    } catch {
      case r: ResourceNotFoundException =>
        throw buildPipelineException(Some(s"Stream $streamName not found"), Some(r), None)
      case r: InvalidArgumentException =>
        throw buildPipelineException(Some(s"Invalid argument for stream $streamName"), Some(r), None)
      case r @ (_: ProvisionedThroughputExceededException | _:  KmsThrottlingException) =>
        if (retryCount < retryPolicy.get.maximumRetries.getOrElse(Constants.TEN)) {
          invokeWaitPeriod(retryPolicy.get, retryCount + 1)
          putRecords(requests, retryCount + 1)
        } else {
          throw buildPipelineException(Some(s"Unable to put records to stream $streamName"), Some(r), None)
        }
      case r @ (_: KmsDisabledException | _:  KmsInvalidStateException | _:  KmsAccessDeniedException |
                _:  KmsNotFoundException | _:  KmsOptInRequiredException) =>
        throw buildPipelineException(Some(s"Unable to put records to stream $streamName as a result of a KMS error"), Some(r), None)
      case t: Throwable =>
        throw buildPipelineException(Some(s"Unable to put records to stream $streamName as a result of an unknown error"), Some(t), None)
    }
  }

  private def getPartitionKey(value: Row): String = partitionKey
    .orElse(partitionKeyIndex.map(i => value.columns(i).toString))
    .filter(s => Option(s).nonEmpty && s.nonEmpty)
    .getOrElse(defaultPartitionKey)
}
