package com.acxiom.gcp.pipeline

import com.acxiom.pipeline._
import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretName, SecretVersion, SecretVersionName}
import org.apache.log4j.Logger

import java.util.Base64
import scala.collection.JavaConverters._

/**
  * An extension of DefaultCredentialProvider which will use the GCP secrets manager to attempt to
  * find Credentials.
  * @param parameters A map containing parameters. projectId is required.
  */
class GCPSecretsManagerCredentialProvider(override val parameters: Map[String, Any])
  extends DefaultCredentialProvider(parameters) {
  private val logger = Logger.getLogger(getClass)
  override protected val defaultParsers = List(new DefaultCredentialParser(), new GCPCredentialParser)
  private val projectId = parameters.getOrElse("projectId", "").asInstanceOf[String]
  private val secretsManagerClient = SecretManagerServiceClient.create()


  override def getNamedCredential(name: String): Option[Credential] = {
    val baseCredential = this.credentials.get(name)

    if (baseCredential.isDefined) {
      baseCredential
    } else {
      try {
        val versions = secretsManagerClient.listSecretVersions(SecretName.of(projectId, name))
        val itr = versions.iterateAll().asScala.toList
        val recent = itr.foldLeft(0)((largest, version) => {
          val num = version.getName.split("/").last
          if (version.getState == SecretVersion.State.ENABLED && num.toInt > largest) {
            num.toInt
          } else {
            largest
          }
        })
        val secret = Option(secretsManagerClient.accessSecretVersion(SecretVersionName.of(projectId, name, recent.toString)))
        if (secret.isDefined &&
          Option(secret.get.getPayload.getData.toStringUtf8).isDefined) {
          Some(DefaultCredential(Map("credentialName" -> name, "credentialValue" -> secret.get.getPayload.getData.toStringUtf8)))
        } else {
          None
        }
      }
      catch {
        case e: Throwable =>
          logger.warn(s"Failed to retrieve secret $name due to: ${e.getMessage}", e)
          None
      }
    }
  }
}

/**
  * An implementation of CredentialParser which returns either a GCPCredential or DefaultCredential
  */
class GCPCredentialParser() extends CredentialParser {
  override def parseCredentials(parameters: Map[String, Any]): List[Credential] = {
    val credentialList = if (parameters.contains("gcpAuthKey")) {
      List(new Base64GCPCredential(parameters))
    } else {
      List()
    }
    if (parameters.contains("credentialName")) {
      val value = parameters("credentialValue").asInstanceOf[String]
      val rawValue = try {
        new String(Base64.getDecoder.decode(value))
      } catch {
        case _: Throwable => value
      }
      // See if this is a Service Account
      if (rawValue.contains("project_id") && rawValue.contains("auth_uri")) {
        credentialList :+ new DefaultGCPCredential(Map[String, Any]("gcpAuthKeyArray" -> rawValue.getBytes))
      } else {
        credentialList :+ DefaultCredential(parameters)
      }
    } else {
      credentialList
    }
  }
}
