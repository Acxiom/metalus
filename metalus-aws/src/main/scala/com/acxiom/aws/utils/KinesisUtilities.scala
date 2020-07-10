package com.acxiom.aws.utils

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}

object KinesisUtilities {
  /**
    * Build a Kinesis client
    * @param region The region
    * @param accessKeyId Optional api key
    * @param secretAccessKey Optional api secret
    * @return A Kinesis Client
    */
  def buildKinesisClient(region: String,
                         accessKeyId: Option[String] = None,
                         secretAccessKey: Option[String] = None): AmazonKinesis = {
    val kinesisClient = if (accessKeyId.isDefined) {
      val credentials = new BasicAWSCredentials(accessKeyId.get, secretAccessKey.getOrElse(""))
      new AmazonKinesisClient(credentials)
    } else {
      new AmazonKinesisClient()
    }
    kinesisClient.setRegion(Region.getRegion(Regions.fromName(region)))
    kinesisClient.setEndpoint(s"https://kinesis.$region.amazonaws.com")
    kinesisClient
  }
}
