package com.acxiom.aws.utils

import com.acxiom.aws.pipeline.connectors.BatchKinesisWriter
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import org.apache.spark.sql.DataFrame

import java.nio.ByteBuffer

object KinesisUtilities {
  /**
    * Build a Kinesis client
    * @param region The region
    * @param credential Optional AWSCredential
    * @return A Kinesis Client
    */
  def buildKinesisClient(region: String,
                         credential: Option[AWSCredential] = None): AmazonKinesis = {
    buildKinesisClientWithCredentials(region, credential.map(_.buildAWSCredentialProvider))
  }

  /**
    * Build a Kinesis client
    *
    * @param region      The region
    * @param accessKeyId Optional api key
    * @param secretAccessKey Optional api secret
    * @return A Kinesis Client
    */
  def buildKinesisClientByKeys(region: String,
                         accessKeyId: Option[String] = None,
                         secretAccessKey: Option[String] = None): AmazonKinesis = {
    buildKinesisClientWithCredentials(region, accessKeyId.map(id => new BasicAWSCredentials(id, secretAccessKey.getOrElse(""))))
  }

  private def buildKinesisClientWithCredentials(region: String, credentials: Option[AWSCredentials]): AmazonKinesis = {
    val builder = AmazonKinesisClient.builder()
    val kinesisClient = credentials.map(c => builder.withCredentials(new AWSStaticCredentialsProvider(c)))
      .getOrElse(builder)
    kinesisClient.withEndpointConfiguration(
      new EndpointConfiguration(s"https://kinesis.$region.amazonaws.com", region))
    kinesisClient.build()
  }

  /**
    * Determines the column id to use to extract the partition key value when writing rows
    * @param dataFrame The DataFrame containing the schema
    * @param partitionKey The field name of the column to use for the key value.
    * @return The column index or zero id the column name is not found.
    */
  def determinePartitionKey(dataFrame: DataFrame, partitionKey: String): Int = {
    if (dataFrame.schema.isEmpty) {
      0
    } else {
      val field = dataFrame.schema.fieldIndex(partitionKey)
      if (field < 0) {
        0
      } else {
        field
      }
    }
  }

  /**
    * Write a single message to a Kinesis Stream
    * @param message The message to post to the Kinesis stream
    * @param region The region of the Kinesis stream
    * @param streamName The name of the Kinesis stream
    * @param partitionKey The key to use when partitioning the message
    * @param accessKeyId The optional API key to use for the Kinesis stream
    * @param secretAccessKey The optional API secret to use for the Kinesis stream
    */
  def postMessage(message: String,
                  region: String,
                  streamName: String,
                  partitionKey: String,
                  accessKeyId: Option[String] = None,
                  secretAccessKey: Option[String] = None): Unit = {
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(streamName)
    putRecordRequest.setPartitionKey(partitionKey)
    putRecordRequest.setData(ByteBuffer.wrap(message.getBytes()))
    val kinesisClient = KinesisUtilities.buildKinesisClientByKeys(region, accessKeyId, secretAccessKey)
    kinesisClient.putRecord(putRecordRequest)
    kinesisClient.shutdown()
  }

  /**
   * Write a single message to a Kinesis Stream
   * @param message The message to post to the Kinesis stream
   * @param region The region of the Kinesis stream
   * @param streamName The name of the Kinesis stream
   * @param partitionKey The key to use when partitioning the message
   * @param credential The optional AWSCredential object use to auth to the Kinesis stream
   */
  def postMessageWithCredentials(message: String,
                                 region: String,
                                 streamName: String,
                                 partitionKey: String,
                                 credential: Option[AWSCredential] = None): Unit = {
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(streamName)
    putRecordRequest.setPartitionKey(partitionKey)
    putRecordRequest.setData(ByteBuffer.wrap(message.getBytes()))
    val kinesisClient = KinesisUtilities.buildKinesisClient(region, credential)
    kinesisClient.putRecord(putRecordRequest)
    kinesisClient.shutdown()
  }

  /**
    * Write a batch DataFrame to Kinesis using record batching.
    * @param dataFrame The DataFrame to write
    * @param region The region of the Kinesis stream
    * @param streamName The Kinesis stream name
    * @param partitionKey The static partition key to use
    * @param partitionKeyIndex The field index in the DataFrame row containing the value to use as the partition key
    * @param separator The field separator to use when formatting the row data
    * @param credential An optional credential to use to authenticate to Kinesis
    */
  def writeDataFrame(dataFrame: DataFrame,
                     region: String,
                     streamName: String,
                     partitionKey: Option[String],
                     partitionKeyIndex: Option[Int],
                     separator: String = ",",
                     credential: Option[AWSCredential] = None): Unit = {
    dataFrame.rdd.foreachPartition(rows => {
      val writer = new BatchKinesisWriter(streamName, region, partitionKey, partitionKeyIndex, separator, credential)
      writer.open()
      rows.foreach(writer.process)
      writer.close()
    })
  }
}
