package com.acxiom.aws.utils

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
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
      new AmazonKinesisClient(credentials)
    } else {
      new AmazonKinesisClient()
    }
    kinesisClient.setRegion(Region.getRegion(Regions.fromName(region)))
    kinesisClient.setEndpoint(s"https://kinesis.$region.amazonaws.com")
    kinesisClient
  }
}
