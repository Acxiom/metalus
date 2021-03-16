package com.acxiom.aws.utils

import com.acxiom.pipeline.Credential
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

trait AWSCredential extends Credential {
  override def name: String = "AWSCredential"
  def awsAccessKey: Option[String]
  def awsAccessSecret: Option[String]
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
}

class AWSCloudWatchCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = "AWSCloudWatchCredential"
  override def awsAccessKey: Option[String] = parameters.get("cloudWatchAccessKeyId").asInstanceOf[Option[String]]
  override def awsAccessSecret: Option[String] = parameters.get("cloudWatchSecretAccessKey").asInstanceOf[Option[String]]
}

class AWSDynamoDBCredential(override val parameters: Map[String, Any]) extends AWSCredential {
  override def name: String = "AWSDynamoDBCredential"
  override def awsAccessKey: Option[String] = parameters.get("dynamoDBAccessKeyId").asInstanceOf[Option[String]]
  override def awsAccessSecret: Option[String] = parameters.get("dynamoDBSecretAccessKey").asInstanceOf[Option[String]]
}
