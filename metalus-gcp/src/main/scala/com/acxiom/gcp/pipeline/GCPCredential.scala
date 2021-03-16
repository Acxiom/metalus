package com.acxiom.gcp.pipeline

import com.acxiom.pipeline.Credential
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

import java.util.Base64

trait GCPCredential extends Credential {
  override def name: String = "GCPCredential"
  def authKey: Map[String, String]
}

/**
  * GCPCredential implementation that looks for the gcpAuthKeyArray parameter to generate the authKey.
  * @param parameters A map containing the gcpAuthKeyArray parameter
  */
class DefaultGCPCredential(override val parameters: Map[String, Any]) extends GCPCredential {
  implicit val formats: Formats = DefaultFormats
  private val authKeyArray = parameters("gcpAuthKeyArray").asInstanceOf[Array[Byte]]
  private val authKeyMap = parse(new String(authKeyArray)).extract[Map[String, String]]
  override def authKey: Map[String, String] = authKeyMap
}

/**
  * GCPCredential implementation that loads the authorization json key from a Base64 string. It is not
  * recommended that this be used in production projects.
  * @param parameters Startup parameters containing the gcpAuthKey parameter.
  */
class Base64GCPCredential(override val parameters: Map[String, Any]) extends GCPCredential {
  implicit val formats: Formats = DefaultFormats
  private val authKeyArray = Base64.getDecoder.decode(parameters("gcpAuthKey").asInstanceOf[String])
  private val authKeyMap = parse(new String(authKeyArray)).extract[Map[String, String]]
  override def authKey: Map[String, String] = authKeyMap
}
