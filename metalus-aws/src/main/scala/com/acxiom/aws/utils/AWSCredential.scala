package com.acxiom.aws.utils

import com.acxiom.pipeline.Credential
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import org.apache.spark.streaming.kinesis.SparkAWSCredentials
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

trait AWSCredential extends Credential {
  override def name: String = "AWSCredential"
  def awsAccessKey: Option[String]
  def awsAccessSecret: Option[String]
  def awsRole: Option[String] = None
  def awsAccountId: Option[String] = None
  def sessionName: Option[String] = None
  def awsPartition: Option[String] = None
  def externalId: Option[String] = None

  def awsRoleARN: Option[String] = {
    val role = awsRole
    val accountId = awsAccountId
    if (role.isDefined && accountId.isDefined) {
      Some(S3Utilities.buildARN(accountId.get, role.get, awsPartition))
    } else {
      None
    }
  }
  // noinspection ScalaStyle
  def buildSparkAWSCredentials = {
    val builder = SparkAWSCredentials.builder
    awsRoleARN.map{ arn =>
      val session = sessionName.getOrElse(s"${awsAccountId.get}_${awsRole.get}")
      externalId.map(id => builder.stsCredentials(arn, session, id)).getOrElse(builder.stsCredentials(arn, session))
    }.getOrElse(builder.basicCredentials(awsAccessKey.get, awsAccessSecret.get)).build()
  }

  def buildAWSCredentialProvider: AWSCredentials = {
    val role = awsRole
    val accountId = awsAccountId
    if (role.isDefined && accountId.isDefined) {
      val sessionCredentials = S3Utilities.assumeRole(accountId.get, role.get, awsPartition, sessionName, externalId).getCredentials
      new BasicSessionCredentials(sessionCredentials.getAccessKeyId,
        sessionCredentials.getSecretAccessKey,
        sessionCredentials.getSessionToken)
    } else {
      new BasicAWSCredentials(awsAccessKey.get, awsAccessSecret.get)
    }
  }
}

class DefaultAWSCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  private implicit val formats: Formats = DefaultFormats
  override def name: String = parameters("credentialName").asInstanceOf[String]
  private val keyMap = parse(parameters("credentialValue").asInstanceOf[String]).extract[Map[String, String]]
  override def awsAccessKey: Option[String] = Some(keyMap.head._1)
  override def awsAccessSecret: Option[String] = Some(keyMap.head._2)
}

class AWSBasicCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def awsAccessKey: Option[String] = parameters.get("accessKeyId").asInstanceOf[Option[String]]
  override def awsAccessSecret: Option[String] = parameters.get("secretAccessKey").asInstanceOf[Option[String]]
  override def awsRole: Option[String] = parameters.get("role").asInstanceOf[Option[String]]
  override def awsAccountId: Option[String] = parameters.get("accountId").asInstanceOf[Option[String]]
  override def sessionName: Option[String] = parameters.get("session").asInstanceOf[Option[String]]
  override def awsPartition: Option[String] = parameters.get("partition").asInstanceOf[Option[String]]
  override def externalId: Option[String] = parameters.get("externalId").asInstanceOf[Option[String]]
}

class AWSCloudWatchCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = "AWSCloudWatchCredential"
  override def awsAccessKey: Option[String] = parameters.get("cloudWatchAccessKeyId").asInstanceOf[Option[String]]
  override def awsAccessSecret: Option[String] = parameters.get("cloudWatchSecretAccessKey").asInstanceOf[Option[String]]
  override def awsRole: Option[String] = parameters.get("cloudWatchRole").asInstanceOf[Option[String]]
  override def awsAccountId: Option[String] = parameters.get("cloudWatchAccountId").asInstanceOf[Option[String]]
  override def sessionName: Option[String] = parameters.get("cloudWatchSession").asInstanceOf[Option[String]]
  override def awsPartition: Option[String] = parameters.get("cloudWatchPartition").asInstanceOf[Option[String]]
  override def externalId: Option[String] = parameters.get("cloudWatchExternalId").asInstanceOf[Option[String]]
}

class AWSDynamoDBCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = "AWSDynamoDBCredential"
  override def awsAccessKey: Option[String] = parameters.get("dynamoDBAccessKeyId").asInstanceOf[Option[String]]
  override def awsAccessSecret: Option[String] = parameters.get("dynamoDBSecretAccessKey").asInstanceOf[Option[String]]
  override def awsRole: Option[String] = parameters.get("dynamoDBRole").asInstanceOf[Option[String]]
  override def awsAccountId: Option[String] = parameters.get("dynamoDBAccountId").asInstanceOf[Option[String]]
  override def sessionName: Option[String] = parameters.get("dynamoDBSession").asInstanceOf[Option[String]]
  override def awsPartition: Option[String] = parameters.get("dynamoDBPartition").asInstanceOf[Option[String]]
  override def externalId: Option[String] = parameters.get("dynamoDBExternalId").asInstanceOf[Option[String]]
}
