package com.acxiom.metalus.utils

import com.acxiom.metalus.Credential
import com.acxiom.metalus.parser.JsonParser

trait AWSCredential extends Credential {
  override def name: String = "AWSCredential"
  def awsAccessKey: Option[String]
  def awsAccessSecret: Option[String]
  def awsRole: Option[String] = None
  def awsAccountId: Option[String] = None
  def sessionName: Option[String] = None
  def awsPartition: Option[String] = None
  def externalId: Option[String] = None
  def duration: Option[String] = None

  def awsRoleARN: Option[String] = {
    val role = awsRole
    val accountId = awsAccountId
    if (role.isDefined && role.get.trim.nonEmpty && accountId.isDefined && accountId.get.trim.nonEmpty) {
      Some(AWSUtilities.buildRoleARN(accountId.get, role.get, awsPartition))
    } else {
      None
    }
  }
  // noinspection ScalaStyle
//  def buildSparkAWSCredentials = {
//    val builder = SparkAWSCredentials.builder
//    awsRoleARN.map{ arn =>
//      val session = sessionName.getOrElse(s"${awsAccountId.get}_${awsRole.get}")
//      externalId.filter(_.trim.nonEmpty).map(id => builder.stsCredentials(arn, session, id)).getOrElse(builder.stsCredentials(arn, session))
//    }.getOrElse(builder.basicCredentials(awsAccessKey.get, awsAccessSecret.get)).build()
//  }

  def buildAWSCredentialInfo: AWSCredentialInfo = {
    val role = awsRole
    val accountId = awsAccountId
    if (role.isDefined && role.get.trim.nonEmpty && accountId.isDefined && accountId.get.trim.nonEmpty) {
      AWSUtilities.assumeRole(accountId.get, role.get, awsPartition, sessionName, externalId)
    } else {
      AWSCredentialInfo(awsAccessKey, awsAccessSecret, None)
    }
  }
}

class DefaultAWSCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = parameters("credentialName").asInstanceOf[String]
  private val keyMap = JsonParser.parseMap(parameters.getOrElse("credentialValue", "{}").asInstanceOf[String]).asInstanceOf[Map[String, String]]
  override def awsRole: Option[String] = if (keyMap.contains("role")) {
    keyMap.get("role")
  } else if (parameters.contains("role")) {
    parameters.get("role").asInstanceOf[Option[String]]
  } else { None }
  override def awsAccountId: Option[String] = if (keyMap.contains("accountId")) {
    keyMap.get("accountId")
  } else if (parameters.contains("accountId")) {
    parameters.get("accountId").asInstanceOf[Option[String]]
  } else { None }
  // See if the key is stored by name, then see if this is a role key and then use the default
  override def awsAccessKey: Option[String] = if (keyMap.contains("accessKeyId")) {
    keyMap.get("accessKeyId")
  } else if ( parameters.contains("accessKeyId")) {
    parameters.get("accessKeyId").asInstanceOf[Option[String]]
  } else if (awsRole.isDefined) {
    None
  } else { Some(keyMap.head._1) }
  override def awsAccessSecret: Option[String] = if (keyMap.contains("secretAccessKey")) {
    keyMap.get("secretAccessKey")
  } else if (parameters.contains("secretAccessKey")) {
    parameters.get("secretAccessKey").asInstanceOf[Option[String]]
  } else if (awsRole.isDefined) {
    None
  } else { Some(keyMap.head._2) }
  override def sessionName: Option[String] = if (keyMap.contains("session")) {
    keyMap.get("session")
  } else if (parameters.contains("session")) {
    parameters.get("session").asInstanceOf[Option[String]]
  } else { None }
  override def awsPartition: Option[String] = if (keyMap.contains("partition")) {
    keyMap.get("partition")
  } else if (parameters.contains("partition")) {
    parameters.get("partition").asInstanceOf[Option[String]]
  } else { None }
  override def externalId: Option[String] = if (keyMap.contains("externalId")) {
    keyMap.get("externalId")
  } else if (parameters.contains("externalId")) {
    parameters.get("externalId").asInstanceOf[Option[String]]
  } else { None }

  override def duration: Option[String] = keyMap.get("duration").orElse(parameters.get("duration").map(_.toString))
}

class AWSBasicCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def awsAccessKey: Option[String] = parameters.get("accessKeyId").map(_.toString)
  override def awsAccessSecret: Option[String] = parameters.get("secretAccessKey").map(_.toString)
  override def awsRole: Option[String] = parameters.get("role").map(_.toString)
  override def awsAccountId: Option[String] = parameters.get("accountId").map(_.toString)
  override def sessionName: Option[String] = parameters.get("session").map(_.toString)
  override def awsPartition: Option[String] = parameters.get("partition").map(_.toString)
  override def externalId: Option[String] = parameters.get("externalId").map(_.toString)
  override def duration: Option[String] = parameters.get("duration").map(_.toString)
}

class AWSCloudWatchCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = "AWSCloudWatchCredential"
  override def awsAccessKey: Option[String] = parameters.get("cloudWatchAccessKeyId").map(_.toString)
  override def awsAccessSecret: Option[String] = parameters.get("cloudWatchSecretAccessKey").map(_.toString)
  override def awsRole: Option[String] = parameters.get("cloudWatchRole").map(_.toString)
  override def awsAccountId: Option[String] = parameters.get("cloudWatchAccountId").map(_.toString)
  override def sessionName: Option[String] = parameters.get("cloudWatchSession").map(_.toString)
  override def awsPartition: Option[String] = parameters.get("cloudWatchPartition").map(_.toString)
  override def externalId: Option[String] = parameters.get("cloudWatchExternalId").map(_.toString)
  override def duration: Option[String] = parameters.get("cloudWatchDuration").map(_.toString)
}

class AWSDynamoDBCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = "AWSDynamoDBCredential"
  override def awsAccessKey: Option[String] = parameters.get("dynamoDBAccessKeyId").map(_.toString)
  override def awsAccessSecret: Option[String] = parameters.get("dynamoDBSecretAccessKey").map(_.toString)
  override def awsRole: Option[String] = parameters.get("dynamoDBRole").map(_.toString)
  override def awsAccountId: Option[String] = parameters.get("dynamoDBAccountId").map(_.toString)
  override def sessionName: Option[String] = parameters.get("dynamoDBSession").map(_.toString)
  override def awsPartition: Option[String] = parameters.get("dynamoDBPartition").map(_.toString)
  override def externalId: Option[String] = parameters.get("dynamoDBExternalId").map(_.toString)
  override def duration: Option[String] = parameters.get("dynamoDBDuration").map(_.toString)
}

case class AWSCredentialInfo(accessKeyId: Option[String], secretAccessKey: Option[String], sessionToken: Option[String])
