package com.acxiom.gcp.steps

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit

import com.acxiom.gcp.utils.GCPCredential
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.steps.ScalaSteps.getClass
import com.acxiom.pipeline.{Constants, PipelineContext}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.threeten.bp.Duration

@StepObject
object PubSubSteps {
  private val logger = Logger.getLogger(getClass)
  private val topicDescription: Some[String] = Some("The topic within the Pub/Sub")
  private val retrySettings = RetrySettings.newBuilder
    .setInitialRetryDelay(Duration.ofMillis(Constants.ONE_HUNDRED))
    .setRetryDelayMultiplier(2.0)
    .setMaxRetryDelay(Duration.ofSeconds(Constants.TWO))
    .setInitialRpcTimeout(Duration.ofSeconds(Constants.TEN))
    .setRpcTimeoutMultiplier(Constants.ONE)
    .setMaxRpcTimeout(Duration.ofMinutes(Constants.ONE))
    .setTotalTimeout(Duration.ofMinutes(Constants.TWO)).build

  @StepFunction("451d4dc8-9bce-4cb4-a91d-1a09e0efd9b8",
    "Write DataFrame to a PubSub Topic",
    "This step will write a DataFrame to a PubSub Topic",
    "Pipeline",
    "GCP")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, topicDescription),
    "separator" -> StepParameter(None, Some(true), None, None, None, None, Some("The separator character to use when combining the column data")),
    "credentials" -> StepParameter(None, Some(true), None, None, None, None, Some("The optional credentials to use for Pub/Sub access"))))
  def writeToStreamWithCredentials(dataFrame: DataFrame,
                    topicName: String,
                    separator: String = ",",
                    credentials: Option[Map[String, String]] = None): Unit = {
    val creds = getCredentials(credentials)
    publishDataFrame(dataFrame, separator, topicName, creds)
  }

  @StepFunction("aaa880e1-4190-4ffe-9fda-4150680f17c9",
    "Write DataFrame to a PubSub Topic Using Global Credentials",
    "This step will write a DataFrame to a PubSub Topic using the CredentialProvider to get Credentials",
    "Pipeline",
    "GCP")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, topicDescription),
    "separator" -> StepParameter(None, Some(true), None, None, None, None, Some("The separator character to use when combining the column data"))))
  def writeToStream(dataFrame: DataFrame,
                    topicName: String,
                    separator: String = ",",
                    pipelineContext: PipelineContext): Unit = {
    val creds = getCredentials(pipelineContext)
    publishDataFrame(dataFrame, separator, topicName, creds)
  }

  @StepFunction("2c937e74-8735-46d6-abfe-0c040ae8f435",
    "Write a single message to a PubSub Topic Using Provided Credentials",
    "This step will write a DataFrame to a PubSub Topic using the providedt Credentials",
    "Pipeline",
    "GCP")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, topicDescription),
    "credentials" -> StepParameter(None, Some(true), None, None, None, None, Some("The optional credentials to use when posting"))))
  def postMessage(message: String, topicName: String, credentials: Option[Map[String, String]] = None): Unit = {
    val creds: _root_.scala.Option[_root_.com.google.auth.oauth2.GoogleCredentials] = getCredentials(credentials)
    publishMessage(topicName, creds, message)
  }

  @StepFunction("b359130d-8e11-44e4-b552-9cef6150bc2b",
    "Write a single message to a PubSub Topic Using Global Credentials",
    "This step will write a message to a PubSub Topic using the CredentialProvider to get Credentials",
    "Pipeline",
    "GCP")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, topicDescription)))
  def postMessage(message: String, topicName: String, pipelineContext: PipelineContext): Unit = {
    publishMessage(topicName, getCredentials(pipelineContext), message)
  }

  /**
    * Publish the DataFrame to the given Pub/Sub topic using the optional credentials
    *
    * @param dataFrame The DataFrame to publish
    * @param topicName The Pub/Sub topic name
    * @param separator The separator to use when combining the column data
    * @param creds     The optional GoogleCredentials
    */
  private def publishDataFrame(dataFrame: DataFrame, topicName: String, separator: String, creds: Option[GoogleCredentials]): Unit = {
    dataFrame.rdd.foreach(row => {
      val rowData = row.mkString(separator)
      publishMessage(topicName, creds, rowData)
    })
  }

  private def publishMessage(topicName: String, creds: Option[GoogleCredentials], message: String) = {
    val publisher = (if (creds.isDefined) {
      Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(creds.get))
    } else {
      Publisher.newBuilder(topicName)
    }).setRetrySettings(retrySettings).build()
    val data = ByteString.copyFromUtf8(message)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
    publisher.publish(pubsubMessage)
    publisher.shutdown()
    publisher.awaitTermination(2, TimeUnit.MINUTES)
  }

  private def getCredentials(pipelineContext: PipelineContext): Option[GoogleCredentials] = {
    if (pipelineContext.credentialProvider.isDefined) {
      val gcpCredential = pipelineContext.credentialProvider.get
        .getNamedCredential("GCPCredential").asInstanceOf[Option[GCPCredential]]
      if (gcpCredential.isDefined) {
        Some(GoogleCredentials.fromStream(new ByteArrayInputStream(gcpCredential.get.authKey))
          .createScoped("https://www.googleapis.com/auth/cloud-platform"))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def getCredentials(credentials: Option[Map[String, String]]) = {
    if (credentials.isDefined) {
      Some(GoogleCredentials.fromStream(
        new ByteArrayInputStream(Serialization.write(credentials)(DefaultFormats).getBytes))
        .createScoped("https://www.googleapis.com/auth/cloud-platform"))
    } else {
      None
    }
  }
}
