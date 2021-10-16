package com.acxiom.pipeline

import com.acxiom.pipeline.api.Authorization
import com.acxiom.pipeline.utils.ReflectionUtils

/**
  * Represents an object that contains named credentials
  */
trait CredentialProvider extends Serializable {
  protected val parameters: Map[String, Any]
  def getNamedCredential(name: String): Option[Credential]
}

/**
  * Represents a single credential
  */
trait Credential extends Serializable {
  protected val parameters: Map[String, Any]
  def name: String
}

/**
  * Creates a credential by parsing the parameters username and password.
  * @param parameters The map containing parameters to scan.
  */
case class UserNameCredential(override val parameters: Map[String, Any]) extends Credential {
  override def name: String = parameters("username").asInstanceOf[String]
  def password: String = parameters("password").asInstanceOf[String]
}

/**
  * Provides an interface for parsing credentials from a map
  */
trait CredentialParser extends Serializable {
  /**
    * Given a map of parameters, parse any credentials.
    * @param parameters Map containing credentials parameters
    * @return A list of crednetials
    */
  def parseCredentials(parameters: Map[String, Any]): List[Credential]
}

/**
  * Default implementation of the CredentialParser. This implementation will load the AuthorizationCredential
  * if the authorization.class parameter is set and any classes listed in the credential-classes parameter.
  */
class DefaultCredentialParser extends CredentialParser {
  override def parseCredentials(parameters: Map[String, Any]): List[Credential] = {
    val baseCredentials = if (parameters.contains("authorization.class")) {
      List(AuthorizationCredential(parameters))
    } else {
      List[Credential]()
    }

   if (parameters.contains("credential-classes")) {
      parameters("credential-classes").asInstanceOf[String]
        .split(',').filter(_.nonEmpty).foldLeft(baseCredentials)((creds, className) => {
        creds :+ ReflectionUtils.loadClass(className, Some(Map("parameters" -> parameters))).asInstanceOf[Credential]
      })
    } else {
      baseCredentials
    }
  }
}

/**
  * Default implementation of the CredentialProvider. This implementation will scan the provided parameters map
  * for the credential-parser and load those in addition to the DefaultCredentialParser which is always used.
  * @param parameters A map containing parameters.
  */
class DefaultCredentialProvider(override val parameters: Map[String, Any]) extends CredentialProvider {
  protected val defaultParsers: List[CredentialParser] = List(new DefaultCredentialParser())
  protected lazy val credentialParsers: List[CredentialParser] = {
    if (parameters.contains("credential-parsers")) {
      parameters("credential-parsers").asInstanceOf[String]
        .split(',').filter(_.nonEmpty).distinct.map(className =>
        ReflectionUtils.loadClass(className, Some(Map("parameters" -> parameters)))
          .asInstanceOf[CredentialParser]).toList ++ defaultParsers
    } else {
      defaultParsers
    }
  }
  protected lazy val credentials: Map[String, Credential] = parseCredentials(parameters)

  override def getNamedCredential(name: String): Option[Credential] = {
    this.credentials.get(name)
  }

  protected def parseCredentials(parameters: Map[String, Any]): Map[String, Credential] = {
    credentialParsers.foldLeft(Map[String, Credential]())((credentials, parser) => {
      val creds = parser.parseCredentials(parameters)
      if (creds.nonEmpty) {
        creds.foldLeft(credentials)((credsMap, credential) => {
          credsMap + (credential.name -> credential)
        })
      } else {
        credentials
      }
    })
  }
}

/**
  * An implementation of Credential that looks for the authorization.class parameter.
  * @param parameters The map containing parameters to scan.
  */
case class AuthorizationCredential(override val parameters: Map[String, Any]) extends Credential {
  private val authorizationClass = "authorization.class"
  private val authorizationParameters = parameters.filter(entry =>
    entry._1.startsWith("authorization.") && entry._1 != authorizationClass)
    .map(entry => entry._1.substring("authorization.".length) -> entry._2)

  override def name: String = "DefaultAuthorization"

  def authorization: Authorization =
    ReflectionUtils.loadClass(
      parameters(authorizationClass).asInstanceOf[String],
      Some(authorizationParameters))
      .asInstanceOf[Authorization]
}

/**
  * A default implementation of Credential. This implementation will use the parameters credentialName and
  * credentialValue to populate the credential.
  * @param parameters The map of parameters to scan.
  */
case class DefaultCredential(override val parameters: Map[String, Any]) extends Credential {
  override def name: String = parameters("credentialName").asInstanceOf[String]
  def value: String = parameters("credentialValue").asInstanceOf[String]
}
