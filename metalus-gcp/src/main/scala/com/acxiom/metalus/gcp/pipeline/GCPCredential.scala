package com.acxiom.metalus.gcp.pipeline

import com.acxiom.metalus.Credential
import com.acxiom.metalus.parser.JsonParser

import java.util.Base64

trait GCPCredential extends Credential {
  override def name: String = "GCPCredential"
  def authKey: Map[String, String]
}

/**
  * An implementation of GCPCredential where parameters represent the full authKey.
  * @param parameters The actual JSON authKey
  */
class BasicGCPCredential(override val parameters: Map[String, Any]) extends GCPCredential {
  override def authKey: Map[String, String] = parameters.map(record => record._1 -> record._2.toString)
}

/**
  * GCPCredential implementation that looks for the gcpAuthKeyArray parameter to generate the authKey.
  * @param parameters A map containing the gcpAuthKeyArray parameter
  */
class DefaultGCPCredential(override val parameters: Map[String, Any]) extends GCPCredential {
  private val authKeyArray = parameters("gcpAuthKeyArray").asInstanceOf[Array[Byte]]
  private val authKeyMap = JsonParser.parseMap(new String(authKeyArray)).asInstanceOf[Map[String, String]]
  override def authKey: Map[String, String] = authKeyMap
}

/**
  * GCPCredential implementation that loads the authorization json key from a Base64 string. It is not
  * recommended that this be used in production projects.
  * @param parameters Startup parameters containing the gcpAuthKey parameter.
  */
class Base64GCPCredential(override val parameters: Map[String, Any]) extends GCPCredential {
  private val authKeyArray = Base64.getDecoder.decode(parameters("gcpAuthKey").asInstanceOf[String])
  private val authKeyMap = JsonParser.parseMap(new String(authKeyArray)).asInstanceOf[Map[String, String]]
  override def authKey: Map[String, String] = authKeyMap
}