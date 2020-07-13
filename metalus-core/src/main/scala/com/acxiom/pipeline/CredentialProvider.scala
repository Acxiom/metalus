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

class DefaultCredentialProvider(override val parameters: Map[String, Any]) extends CredentialProvider {
  private val credentials = if (parameters.contains("credential-classes")) {
    parameters("credential-classes").asInstanceOf[String].split(',').map(className => {
      val credential = ReflectionUtils.loadClass(className, Some(Map("parameters" -> parameters))).asInstanceOf[Credential]
      credential.name -> credential
    }).toMap
  } else if (parameters.contains("authorization.class")) {
    val credential = AuthorizationCredential(parameters)
    Map[String, Credential](credential.name -> credential)
  } else {
    Map[String, Credential]()
  }

  override def getNamedCredential(name: String): Option[Credential] = {
    this.credentials.get(name)
  }
}

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
