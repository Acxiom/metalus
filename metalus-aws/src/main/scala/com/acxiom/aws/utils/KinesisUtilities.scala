package com.acxiom.aws.utils

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}

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
    if (credential.isDefined) {
      KinesisUtilities.buildKinesisClientByKeys(region, credential.get.awsAccessKey, credential.get.awsAccessSecret)
    } else {
      KinesisUtilities.buildKinesisClientByKeys(region)
    }
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
    val kinesisClient = if (accessKeyId.isDefined) {
      val credentials = new BasicAWSCredentials(accessKeyId.getOrElse(""), secretAccessKey.getOrElse(""))
      AmazonKinesisClient.builder().withCredentials(new AWSStaticCredentialsProvider(credentials))
    } else {
      AmazonKinesisClient.builder()
    }
    kinesisClient.withRegion(Regions.fromName(region))
    kinesisClient.withEndpointConfiguration(
      new EndpointConfiguration(s"https://kinesis.$region.amazonaws.com", region))
    kinesisClient.build()
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
}
