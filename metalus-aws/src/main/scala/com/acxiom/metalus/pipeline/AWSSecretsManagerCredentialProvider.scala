package com.acxiom.metalus.pipeline

import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.utils.{AWSBasicCredential, AWSCloudWatchCredential, AWSDynamoDBCredential, DefaultAWSCredential}
import com.acxiom.metalus.{Credential, CredentialParser, DefaultCredentialParser, DefaultCredentialProvider}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{DecryptionFailureException, GetSecretValueRequest, InvalidParameterException, InvalidRequestException, ResourceNotFoundException}

import java.net.URI

class AWSSecretsManagerCredentialProvider(override val parameters: Map[String, Any])
  extends DefaultCredentialProvider(parameters) {
  private val logger = LoggerFactory.getLogger(getClass)
  override protected val defaultParsers: List[CredentialParser] = List(new DefaultCredentialParser(), new AWSCredentialParser)
  val region: String = parameters.getOrElse("region", "us-east-1").asInstanceOf[String]
  private val client = SecretsManagerClient.builder().endpointOverride(new URI(s"secretsmanager.$region.amazonaws.com")).build()

  override def getNamedCredential(name: String): Option[Credential] = {
    val basicCredential = this.credentials.get(name)

    if (basicCredential.isDefined) {
      basicCredential
    } else {
      try {
        val getSecretValueResult = Option(client.getSecretValue(GetSecretValueRequest.builder()
          .secretId(name).versionStage("AWSCURRENT").build()))
        val secretString = if (getSecretValueResult.isDefined &&
          Option(getSecretValueResult.get.secretString()).isDefined) {
          getSecretValueResult.get.secretString()
        } else {
          getSecretValueResult.get.secretBinary().toString
        }
        val keyMap = JsonParser.parseMap(secretString).asInstanceOf[Map[String, String]]
        val creds = parseCredentials(keyMap)
        Some(if (creds.contains("AWSCredential")) {
          creds("AWSCredential")
        } else {
          new DefaultAWSCredential(Map("credentialName" -> name, "credentialValue" -> secretString))
        })
      } catch {
        case _: ResourceNotFoundException => None
        case e @ (_: InvalidRequestException | _: InvalidParameterException) =>
          logger.warn(s"The request had invalid params: ${e.getMessage}", e)
          None
        case e: DecryptionFailureException =>
          logger.warn(s"The key could not be decrypted: ${e.getMessage}", e)
          None
        case e: Throwable =>
          logger.warn(s"Unknown exception occurred while retrieving secret: ${e.getMessage}", e)
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
