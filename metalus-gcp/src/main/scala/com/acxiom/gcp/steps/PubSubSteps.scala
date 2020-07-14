package com.acxiom.gcp.steps

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.threeten.bp.Duration

@StepObject
object PubSubSteps {
  private val retrySettings = RetrySettings.newBuilder
    .setInitialRetryDelay(Duration.ofMillis(100))
    .setRetryDelayMultiplier(2.0)
    .setMaxRetryDelay(Duration.ofSeconds(2))
    .setInitialRpcTimeout(Duration.ofSeconds(10))
    .setRpcTimeoutMultiplier(1)
    .setMaxRpcTimeout(Duration.ofMinutes(1))
    .setTotalTimeout(Duration.ofMinutes(2)).build

  @StepFunction("451d4dc8-9bce-4cb4-a91d-1a09e0efd9b8",
    "Write DataFrame to a PubSub Topic",
    "This step will write a DataFrame to a PubSub Topic",
    "Pipeline",
    "GCP")
  def writeToStream(dataFrame: DataFrame,
                    topicName: String,
                    separator: String = ",",
                    credentials: Option[Map[String, String]] = None): Unit = {
    val creds = if (credentials.isDefined) {
      Some(GoogleCredentials.fromStream(
        new ByteArrayInputStream(Serialization.write(credentials)(DefaultFormats).getBytes))
        .createScoped("https://www.googleapis.com/auth/cloud-platform"))
    } else {
      None
    }
    dataFrame.rdd.foreach(row => {
      val rowData = row.mkString(separator)
      val publisher = (if (creds.isDefined) {
        Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(creds.get))
      } else {
        Publisher.newBuilder(topicName)
      }).setRetrySettings(retrySettings).build()
      val data = ByteString.copyFromUtf8(rowData)
      val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
      publisher.publish(pubsubMessage)
      publisher.shutdown()
      publisher.awaitTermination(2, TimeUnit.MINUTES)
    })
  }
}
