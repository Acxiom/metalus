package com.acxiom.metalus.gcp.utils

import com.acxiom.metalus.connectors.{DataRowReader, DataRowWriter}
import com.acxiom.metalus.drivers.StreamingDataParser
import com.acxiom.metalus.gcp.pipeline.{BasicGCPCredential, GCPCredential}
import com.acxiom.metalus.gcp.utils.GCPUtilities.{generateCredentialsByteArray, retrySettings}
import com.acxiom.metalus.sql.Row
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.{Constants, CredentialProvider, PipelineContext}
import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1._
import org.threeten.bp.Duration

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object GCPUtilities {
  type PubSubCallback = PartialFunction[Try[String], Unit]

  private val requestBytesThreshold = 5000L
  private val messageCountBatchSize = 100L
  private[gcp] val batchingSettings: BatchingSettings = BatchingSettings.newBuilder
    .setElementCountThreshold(messageCountBatchSize)
    .setRequestByteThreshold(requestBytesThreshold)
    .setDelayThreshold(Duration.ofMillis(Constants.ONE_HUNDRED)).build
  private[gcp] val retrySettings: RetrySettings = RetrySettings.newBuilder
    .setInitialRetryDelay(Duration.ofMillis(Constants.ONE_HUNDRED))
    .setRetryDelayMultiplier(2.0)
    .setMaxRetryDelay(Duration.ofSeconds(Constants.TWO))
    .setInitialRpcTimeout(Duration.ofSeconds(Constants.TEN))
    .setRpcTimeoutMultiplier(Constants.ONE)
    .setMaxRpcTimeout(Duration.ofMinutes(Constants.ONE))
    .setTotalTimeout(Duration.ofMinutes(Constants.TWO)).build

  //  /**
  //    * Given a credential map, this function will set the appropriate properties required for GCS access from Spark.
  //    *
  //    * @param credentials     The GCP auth map
  //    * @param pipelineContext The current pipeline context
  //    */
  //  def setGCSAuthorization(credentials: Map[String, String], pipelineContext: PipelineContext): Unit = {
  //    val skipGCSFS = pipelineContext.getGlobalAs[Boolean]("skipGCSFS")
  //    if (!skipGCSFS.getOrElse(false)) {
  //      val sc = pipelineContext.sparkSession.get.sparkContext
  //      sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  //      sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
  //    }
  //    setGCPSecurity(credentials, pipelineContext)
  //  }

  //  /**
  //    * Given a credential map, this function will set the security properties required for Spark access.
  //    * @param credentials     The GCP auth map
  //    * @param pipelineContext The current pipeline context
  //    */
  //  def setGCPSecurity(credentials: Map[String, String], pipelineContext: PipelineContext): Unit = {
  //    val sc = pipelineContext.sparkSession.get.sparkContext
  //    // Private Key
  //    sc.hadoopConfiguration.set("fs.gs.project.id", credentials("project_id"))
  //    sc.hadoopConfiguration.set("fs.gs.auth.service.account.enable", "true")
  //    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email", credentials("client_email"))
  //    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", credentials("private_key_id"))
  //    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", credentials("private_key"))
  //  }

  /**
   * Given a credential map, convert to a BasicGCPCredential.
   *
   * @param credentials The map containing the json based credentials
   * @return A BasicGCPCredential that can be passed to connector code.
   */
  def convertMapToCredential(credentials: Option[Map[String, String]]): Option[BasicGCPCredential] = {
    credentials.map(new BasicGCPCredential(_))
  }

  /**
   * Retrieve the credentials needed to interact with GCP services.
   *
   * @param credentialProvider The credential provider
   * @param credentialName     The name of the credential
   * @return An optional GoogleCredentials object
   */
  private[gcp] def getCredentialsFromCredentialProvider(credentialProvider: CredentialProvider,
                                                          credentialName: String = "GCPCredential"): Option[GoogleCredentials] = {
    val gcpCredential = credentialProvider
      .getNamedCredential(credentialName).asInstanceOf[Option[GCPCredential]]
    generateCredentials(gcpCredential.map(_.authKey))
  }

  /**
   * Retrieve the credentials needed to interact with GCP services.
   *
   * @param pipelineContext The pipeline context containing the credential provider
   * @param credentialName  The name of the credential
   * @return An optional GoogleCredentials object
   */
  private[gcp] def getCredentialsFromPipelineContext(pipelineContext: PipelineContext,
                                                       credentialName: String = "GCPCredential"): Option[GoogleCredentials] = {
    pipelineContext.credentialProvider.flatMap(getCredentialsFromCredentialProvider(_, credentialName))
  }

  /**
   * Retrieve the credentials needed to interact with GCP services.
   *
   * @param credentials TA map containing the Google credentials
   * @return An optional GoogleCredentials object
   */
  private[gcp] def generateCredentials(credentials: Option[Map[String, String]]): Option[GoogleCredentials] = {
    generateCredentialsByteArray(credentials).map { cred =>
      GoogleCredentials.fromStream(new ByteArrayInputStream(cred))
        .createScoped("https://www.googleapis.com/auth/cloud-platform")
    }
  }

  /**
   * Given a credentials map, return a byte array
   *
   * @param credentials The credentials map
   * @return A byte array or none
   */
  def generateCredentialsByteArray(credentials: Option[Map[String, String]]): Option[Array[Byte]] = {
    // Don't use json4s to build the auth byte array, it can malform the private key.
    credentials.map { cred =>
      s"{${cred.map { case (k, v) => s""""$k": "$v"""" }.mkString(",")}}".getBytes
    }
  }

  /**
   * @param projectId The project id for this topic
   * @param topicId   the topicId to use
   * @return A string topic name for the projectId and topicId provided.
   */
  def getTopicName(projectId: String, topicId: String): String =
    ProjectTopicName.of(projectId, topicId).toString

  /**
   * Write a single message to a PubSub Topic using the provided credentials
   *
   * @param topicName   The topic within the Pub/Sub
   * @param message     The message to post to the Pub/Sub topic
   * @param attributes  Optional map of attributes to add to the message
   * @param credentials The credentials needed to post the message
   * @return A boolean indicating whether the message was published
   */
  def postMessage(topicName: String, message: String, attributes: Option[Map[String, String]] = None,
                  credentials: Option[Map[String, String]] = None): Boolean = {
    postMessageInternal(topicName, generateCredentials(credentials), message, attributes)
  }

  /**
   * Write a single message to a PubSub Topic using the provided credentials,
   * with the provided function used as a callback.
   * callback can be a partial function, all unmatched values will be ignored.
   *
   * @param topicName   The topic within the Pub/Sub
   * @param message     The message to post to the Pub/Sub topic
   * @param attributes  Optional map of attributes to add to the message
   * @param credentials The credentials needed to post the message
   * @param callback    a callback function that will be passed either a Success(messageId) or Failure(throwable)
   *                    once the message is published.
   * @return A boolean indicating whether the message was published
   */
  def postMessageWithCallback(topicName: String, message: String, attributes: Option[Map[String, String]] = None,
                              credentials: Option[Map[String, String]] = None)
                             (callback: PubSubCallback): Unit = {
    postMessageInternal(topicName, generateCredentials(credentials), message, attributes, Some(callback))
  }

  /**
   * Write a single message to a PubSub Topic using the provided credentials
   *
   * @param topicName    The topic within the Pub/Sub
   * @param creds        The credentials needed to post the message
   * @param message      The message to post to the Pub/Sub topic
   * @param attributes   Optional map of attributes to add to the message
   * @param callbackFunc Optional function that will be passed either a Success(messageId) or Failure(throwable)
   *                     once the message is published.
   * @return A boolean indicating whether the message was published
   */
  private[gcp] def postMessageInternal(topicName: String, creds: Option[GoogleCredentials], message: String,
                                         attributes: Option[Map[String, String]] = None,
                                         callbackFunc: Option[PubSubCallback] = None): Boolean = {
    val publisher = getPublisherBuilder(topicName, creds).setRetrySettings(retrySettings).build()
    val data = ByteString.copyFromUtf8(message)
    val builder = PubsubMessage.newBuilder.setData(data)
    val pubsubMessage = attributes.map(_.asJava)
      .map(builder.putAllAttributes)
      .getOrElse(builder)
      .setData(data)
      .build()
    val future = publisher.publish(pubsubMessage)
    callbackFunc.map { func =>
      ApiFutures.addCallback(future, new ApiFutureCallback[String]() {
        override def onFailure(throwable: Throwable): Unit = {
          try {
            func.lift(Failure(throwable))
          }
          publisher.shutdown()
        }

        override def onSuccess(messageId: String): Unit = {
          try {
            func.lift(Success(messageId))
          }
          publisher.shutdown()
        }
      }, MoreExecutors.directExecutor)
      true
    } getOrElse {
      publisher.shutdown()
      publisher.awaitTermination(2, TimeUnit.MINUTES)
    }
  }

  /**
   * Generates the builder used to create a publisher for Pub/Sub messages.
   *
   * @param topicName The topic within the Pub/Sub
   * @param creds     The credentials needed to post the message
   * @return
   */
  private[gcp] def getPublisherBuilder(topicName: String, creds: Option[GoogleCredentials]): Publisher.Builder = {
    if (creds.isDefined) {
      Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(creds.get))
    } else {
      Publisher.newBuilder(topicName)
    }
  }
}

case class PubSubDataRowReader(projectId: String,
                               subscriptionId: String,
                               batchSize: Int,
                               parsers: List[StreamingDataParser[ReceivedMessage]],
                               credential: Option[GCPCredential] = None) extends DataRowReader {
  private lazy val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
  private lazy val provider = SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
    .setMaxInboundMessageSize(20 << 20) // TODO Is 20 MB good?
    .build()
  private lazy val subscriberStubSettings = if (credential.isDefined) {
    val creds = generateCredentialsByteArray(Some(credential.get.authKey)).map { cred =>
      GoogleCredentials.fromStream(new ByteArrayInputStream(cred))
        .createScoped("https://www.googleapis.com/auth/cloud-platform")
    }
    SubscriberStubSettings.newBuilder()
      .setCredentialsProvider(FixedCredentialsProvider.create(creds.get))
      .setTransportChannelProvider(provider).build()
  } else {
    SubscriberStubSettings.newBuilder().setTransportChannelProvider(provider).build()
  }
  private lazy val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)
  private lazy val pullRequest = PullRequest.newBuilder().setMaxMessages(batchSize)
    .setSubscription(subscriptionName).build()

  override def next(): Option[List[Row]] = {
    val pullResponse = subscriber.pullCallable().call(pullRequest)
    if (pullResponse.getReceivedMessagesCount > 0) {
      val messages = pullResponse.getReceivedMessagesList.asScala
      val parser = DriverUtils.getStreamingParser(messages.head, parsers)
      // Convert the data to Row and hold the ackId so they can be acknowledged
      val rows = parser.get.parseRecords(messages.toList)
      // ACK the messages that we have read
      val acknowledgeRequest = AcknowledgeRequest.newBuilder().setSubscription(subscriptionName)
        .addAllAckIds(messages.map(_.getAckId).asJava).build()
      subscriber.acknowledgeCallable().call(acknowledgeRequest)
      Some(rows)
    } else {
      Some(List())
    }
  }

  override def close(): Unit = {
    subscriber.shutdown()
  }

  override def open(): Unit = {}
}

case class PubSubDataRowWriter(topicName: String,
                               separator: String = ",",
                               attributes: Option[Map[String, String]] = None,
                               credential: Option[GCPCredential] = None) extends DataRowWriter {

  private lazy val creds = GCPUtilities.generateCredentials(Some(credential.get.authKey))
  private lazy val publisher = GCPUtilities.getPublisherBuilder(topicName, creds)
    .setBatchingSettings(GCPUtilities.batchingSettings)
    .setRetrySettings(retrySettings).build()

  override def process(rows: List[Row]): Unit = {
    val messageIdFutures = rows.map(row => {
      val data = ByteString.copyFromUtf8(row.mkString(separator))
      val builder = PubsubMessage.newBuilder.setData(data)
      val pubsubMessage = attributes.map(_.asJava)
        .map(builder.putAllAttributes)
        .getOrElse(builder)
        .setData(data)
        .build()
      publisher.publish(pubsubMessage)
    })
    ApiFutures.allAsList(messageIdFutures.asJava).get()
  }

  override def close(): Unit = {
    publisher.publishAllOutstanding()
    publisher.shutdown()
    publisher.awaitTermination(2, TimeUnit.MINUTES)
  }

  override def open(): Unit = {}
}
