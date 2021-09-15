package com.acxiom.gcp.utils

import com.acxiom.gcp.pipeline.GCPCredential
import com.acxiom.pipeline.{Constants, CredentialProvider, PipelineContext}
import com.google.api.gax.batching.BatchingSettings
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.threeten.bp.Duration

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit

object GCPUtilities {
  val requestBytesThreshold = 5000L
  val messageCountBatchSize = 100L
  val batchingSettings: BatchingSettings = BatchingSettings.newBuilder
    .setElementCountThreshold(messageCountBatchSize)
    .setRequestByteThreshold(requestBytesThreshold)
    .setDelayThreshold(Duration.ofMillis(Constants.ONE_HUNDRED)).build
  val retrySettings: RetrySettings = RetrySettings.newBuilder
    .setInitialRetryDelay(Duration.ofMillis(Constants.ONE_HUNDRED))
    .setRetryDelayMultiplier(2.0)
    .setMaxRetryDelay(Duration.ofSeconds(Constants.TWO))
    .setInitialRpcTimeout(Duration.ofSeconds(Constants.TEN))
    .setRpcTimeoutMultiplier(Constants.ONE)
    .setMaxRpcTimeout(Duration.ofMinutes(Constants.ONE))
    .setTotalTimeout(Duration.ofMinutes(Constants.TWO)).build

  /**
    * Given a credential map, this function will set the appropriate properties required for Spark access.
    *
    * @param credentials     The GCP auth map
    * @param pipelineContext The current pipeline context
    */
  def setGCSAuthorization(credentials: Map[String, String], pipelineContext: PipelineContext): Unit = {
    val sc = pipelineContext.sparkSession.get.sparkContext
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    // Private Key
    sc.hadoopConfiguration.set("fs.gs.project.id", credentials("project_id"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.enable", "true")
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email", credentials("client_email"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", credentials("private_key_id"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", credentials("private_key"))
  }

  /**
    * Retrieve the credentials needed to interact with GCP services.
    * @param credentialProvider The credential provider
    * @param credentialName The name of the credential
    * @return An optional GoogleCredentials object
    */
  def getCredentialsFromCredentialProvider(credentialProvider: CredentialProvider,
                                           credentialName: String = "GCPCredential"): Option[GoogleCredentials] = {
    val gcpCredential = credentialProvider
      .getNamedCredential(credentialName).asInstanceOf[Option[GCPCredential]]
    if (gcpCredential.isDefined) {
      generateCredentials(Some(gcpCredential.get.authKey))
    } else {
      None
    }
  }

  /**
    * Retrieve the credentials needed to interact with GCP services.
    * @param pipelineContext The pipeline context containing the credential provider
    * @param credentialName The name of the credential
    * @return An optional GoogleCredentials object
    */
  def getCredentialsFromPipelineContext(pipelineContext: PipelineContext,
                                        credentialName: String = "GCPCredential"): Option[GoogleCredentials] = {
    if (pipelineContext.credentialProvider.isDefined) {
      this.getCredentialsFromCredentialProvider(pipelineContext.credentialProvider.get, credentialName)
    } else {
      None
    }
  }

  /**
    * Retrieve the credentials needed to interact with GCP services.
    * @param credentials TA map containing the Google credentials
    * @return An optional GoogleCredentials object
    */
  def generateCredentials(credentials: Option[Map[String, String]]): Option[GoogleCredentials] = {
    if (credentials.isDefined) {
      Some(GoogleCredentials.fromStream(
        new ByteArrayInputStream(Serialization.write(credentials)(DefaultFormats).getBytes))
        .createScoped("https://www.googleapis.com/auth/cloud-platform"))
    } else {
      None
    }
  }

  /**
    * Given a credentials map, return a byte array
    * @param credentials The credentials map
    * @return A byte array or none
    */
  def generateCredentialsByteArray(credentials: Option[Map[String, String]]): Option[Array[Byte]] = {
    if (credentials.isDefined) {
      Some(Serialization.write(credentials)(DefaultFormats).getBytes)
    } else {
      None
    }
  }

  /**
    * Write a single message to a PubSub Topic using the provided credentials
    * @param topicName The topic within the Pub/Sub
    * @param creds The credentials needed to post the message
    * @param message The message to post to the Pub/Sub topic
    * @return A boolean indicating whether the message was published
    */
  def postMessage(topicName: String, creds: Option[GoogleCredentials], message: String): Boolean = {
    val publisher = getPublisherBuilder(topicName, creds).setRetrySettings(retrySettings).build()
    val data = ByteString.copyFromUtf8(message)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
    publisher.publish(pubsubMessage)
    publisher.shutdown()
    publisher.awaitTermination(2, TimeUnit.MINUTES)
  }

  /**
    * Generates the builder used to create a publisher for Pub/Sub messages.
    * @param topicName The topic within the Pub/Sub
    * @param creds The credentials needed to post the message
    * @return
    */
  def getPublisherBuilder(topicName: String, creds: Option[GoogleCredentials]): Publisher.Builder = {
    if (creds.isDefined) {
      Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(creds.get))
    } else {
      Publisher.newBuilder(topicName)
    }
  }
}
