package com.acxiom.aws.utils

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}

object KinesisUtilities {
  /**
    * Build a Kinesis client
    * @param region The region
    * @param credential Optional AWSCrednetial
    * @return A Kinesis Client
    */
  def buildKinesisClient(region: String,
                         credential: Option[AWSCredential] = None): AmazonKinesis = {
    val kinesisClient = if (credential.isDefined) {
      val credentials = new BasicAWSCredentials(credential.get.awsAccessKey.getOrElse(), credential.get.awsAccessSecret.getOrElse(""))
      new AmazonKinesisClient(credentials)
    } else {
      new AmazonKinesisClient()
    }
    kinesisClient.setRegion(Region.getRegion(Regions.fromName(region)))
    kinesisClient.setEndpoint(s"https://kinesis.$region.amazonaws.com")
    kinesisClient
  }
}
