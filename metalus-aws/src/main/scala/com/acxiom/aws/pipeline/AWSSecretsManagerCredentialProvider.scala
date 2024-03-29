package com.acxiom.aws.pipeline

import com.acxiom.aws.utils.{AWSBasicCredential, AWSCloudWatchCredential, AWSDynamoDBCredential, DefaultAWSCredential}
import com.acxiom.pipeline.{Credential, CredentialParser, DefaultCredentialParser, DefaultCredentialProvider}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, InvalidParameterException, InvalidRequestException, ResourceNotFoundException}
import org.apache.log4j.Logger
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

class AWSSecretsManagerCredentialProvider(override val parameters: Map[String, Any])
  extends DefaultCredentialProvider(parameters) {
  private val logger = Logger.getLogger(getClass)
  private implicit val formats: Formats = DefaultFormats
  override protected val defaultParsers = List(new DefaultCredentialParser(), new AWSCredentialParser)
  val region: String = parameters.getOrElse("region", "us-east-1").asInstanceOf[String]
  private val config = new AwsClientBuilder.EndpointConfiguration(s"secretsmanager.$region.amazonaws.com", region)
  private val clientBuilder = AWSSecretsManagerClientBuilder.standard
  clientBuilder.setEndpointConfiguration(config)
  private val client = clientBuilder.build()

  override def getNamedCredential(name: String): Option[Credential] = {
    val basicCredential = this.credentials.get(name)

    if (basicCredential.isDefined) {
      basicCredential
    } else {
      try {
        val getSecretValueResult = Option(client.getSecretValue(new GetSecretValueRequest()
          .withSecretId(name).withVersionStage("AWSCURRENT")))
        val secretString = if (getSecretValueResult.isDefined &&
          Option(getSecretValueResult.get.getSecretString).isDefined) {
          getSecretValueResult.get.getSecretString
        } else {
          getSecretValueResult.get.getSecretBinary.toString
        }
        val keyMap = parse(secretString).extract[Map[String, String]]
        val creds = parseCredentials(keyMap)
        Some(if (creds.contains("AWSCredential")) {
          creds("AWSCredential")
        } else {
          new DefaultAWSCredential(Map("credentialName" -> name, "credentialValue" -> secretString))
        })
      }
      catch {
        case _: ResourceNotFoundException => None
        case e: InvalidRequestException =>
          logger.warn(s"The request was invalid due to: ${e.getMessage}")
          None
        case e: InvalidParameterException =>
          logger.warn(s"The request had invalid params: ${e.getMessage}")
          None
        case e: Throwable =>
          logger.warn(s"Unknown exception occurred while retrieving secret: ${e.getMessage}")
          None
      }
    }
  }
}

class AWSCredentialParser extends CredentialParser {
  /**
    * Given a map of parameters, parse any credentials.
    *
    * @param parameters Map containing credentials parameters
    * @return A list of credentials
    */
  override def parseCredentials(parameters: Map[String, Any]): List[Credential] = {
    parameters.foldLeft(List[Credential]())((credentials, param) => {
      param._1 match {
        case "accessKeyId" => credentials :+ new AWSBasicCredential(parameters)
        case "accountId" if !parameters.contains("accessKeyId") => credentials :+ new AWSBasicCredential(parameters)
        case "cloudWatchAccessKeyId" => credentials :+ new AWSCloudWatchCredential(parameters)
        case "dynamoDBAccessKeyId" => credentials :+ new AWSDynamoDBCredential(parameters)
        case _ => credentials
      }
    })
  }
}
