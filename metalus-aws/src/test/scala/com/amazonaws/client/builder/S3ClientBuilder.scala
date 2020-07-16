package com.amazonaws.client.builder

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

/**
  * This object exists to allow building a the client that allows the end point to be set. This is needed to continue
  * to support Spark 2.3.
  */
object S3ClientBuilder {
  def getS3TestClient(region: String): AmazonS3 = {
    val builder = AmazonS3ClientBuilder.standard
      .withRegion(region)
      .withPathStyleAccessEnabled(true)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .asInstanceOf[AwsSyncClientBuilder[AmazonS3ClientBuilder, AmazonS3]]

      builder.build(builder.getSyncClientParams)
  }
}
