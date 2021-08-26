package com.acxiom.aws.pipeline

import com.acxiom.aws.utils.{AWSBasicCredential, AWSCloudWatchCredential, AWSDynamoDBCredential, DefaultAWSCredential}
import com.acxiom.pipeline.{Credential, CredentialParser, DefaultCredentialProvider}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, InvalidParameterException, InvalidRequestException, ResourceNotFoundException}
import org.apache.log4j.Logger

class AWSSecretsManagerCredentialProvider(val params: Map[String, Any])
  extends DefaultCredentialProvider(params +
    ("credential-parsers" -> s"${params.getOrElse("credential-parsers", "")},com.acxiom.aws.pipeline.AWSCredentialParser")) {
  private val logger = Logger.getLogger(getClass)
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
        Some(if (getSecretValueResult.isDefined &&
          Option(getSecretValueResult.get.getSecretString).isDefined) {
          new DefaultAWSCredential(Map("credentialName" -> name, "credentialValue" -> getSecretValueResult.get.getSecretString))
        } else {
          new DefaultAWSCredential(Map("credentialName" -> name, "credentialValue" -> getSecretValueResult.get.getSecretBinary.toString))
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
        case "cloudWatchAccessKeyId" => credentials :+ new AWSCloudWatchCredential(parameters)
        case "dynamoDBAccessKeyId" => credentials :+ new AWSDynamoDBCredential(parameters)
        case _ => credentials
      }
    })
  }
}
