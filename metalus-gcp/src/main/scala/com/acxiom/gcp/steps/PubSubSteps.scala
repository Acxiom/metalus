package com.acxiom.gcp.steps

import com.acxiom.gcp.utils.GCPUtilities
import com.acxiom.gcp.utils.GCPUtilities.getPublisherBuilder
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.google.auth.oauth2.GoogleCredentials
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.spark.sql.DataFrame

import java.util.concurrent.TimeUnit

@StepObject
object PubSubSteps {
  @StepFunction("451d4dc8-9bce-4cb4-a91d-1a09e0efd9b8",
    "Write DataFrame to a PubSub Topic",
    "This step will write a DataFrame to a PubSub Topic",
    "Pipeline",
    "GCP")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, Some("The topic within the Pub/Sub")),
    "separator" -> StepParameter(None, Some(false), None, None, None, None, Some("The separator character to use when combining the column data")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional credentials to use for Pub/Sub access"))))
  def writeToStreamWithCredentials(dataFrame: DataFrame,
                                   topicName: String,
                                   separator: String = ",",
                                   credentials: Option[Map[String, String]] = None): Unit = {
    val creds = GCPUtilities.generateCredentials(credentials)
    publishDataFrame(dataFrame, separator, topicName, creds)
  }

  @StepFunction("aaa880e1-4190-4ffe-9fda-4150680f17c9",
    "Write DataFrame to a PubSub Topic Using Global Credentials",
    "This step will write a DataFrame to a PubSub Topic using the CredentialProvider to get Credentials",
    "Pipeline",
    "GCP")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, Some("The topic within the Pub/Sub")),
    "separator" -> StepParameter(None, Some(false), None, None, None, None, Some("The separator character to use when combining the column data"))))
  def writeToStream(dataFrame: DataFrame,
                    topicName: String,
                    separator: String = ",",
                    pipelineContext: PipelineContext): Unit = {
    val creds = GCPUtilities.getCredentialsFromPipelineContext(pipelineContext)
    publishDataFrame(dataFrame, separator, topicName, creds)
  }

  @StepFunction("2c937e74-8735-46d6-abfe-0c040ae8f435",
    "Write a single message to a PubSub Topic Using Provided Credentials",
    "This step will write a DataFrame to a PubSub Topic using the provided Credentials",
    "Pipeline",
    "GCP")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, Some("The topic within the Pub/Sub")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional credentials to use when posting")),
    "attributes" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional message attributes to set when posting"))))
  def postMessage(message: String, topicName: String, credentials: Option[Map[String, String]], attributes: Option[Map[String, String]]): Unit = {
    GCPUtilities.postMessage(topicName, message, attributes, credentials)
  }

  @StepFunction("b359130d-8e11-44e4-b552-9cef6150bc2b",
    "Write a single message to a PubSub Topic Using Global Credentials",
    "This step will write a message to a PubSub Topic using the CredentialProvider to get Credentials",
    "Pipeline",
    "GCP")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Pub/Sub topic")),
    "topicName" -> StepParameter(None, Some(true), None, None, None, None, Some("The topic within the Pub/Sub")),
    "attributes" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional message attributes to set when posting"))))
  def postMessage(message: String, topicName: String, attributes: Option[Map[String, String]], pipelineContext: PipelineContext): Unit = {
    GCPUtilities.postMessageInternal(topicName, GCPUtilities.getCredentialsFromPipelineContext(pipelineContext), message, attributes)
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
    dataFrame.rdd.foreachPartition(iter => {
      val publisher = getPublisherBuilder(topicName, creds)
        .setRetrySettings(GCPUtilities.retrySettings).setBatchingSettings(GCPUtilities.batchingSettings).build()
      iter.foreach(row => {
        val data = row.mkString(separator)
        val pubsubMessage = PubsubMessage.newBuilder.setData(ByteString.copyFromUtf8(data)).build
        publisher.publish(pubsubMessage)
      })
      publisher.shutdown()
      publisher.awaitTermination(2, TimeUnit.MINUTES)
    })
  }
}
