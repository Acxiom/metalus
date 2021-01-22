package com.acxiom.aws.utils

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}

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
}
