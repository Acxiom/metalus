package com.acxiom.gcp.drivers

import java.util.Base64

import com.acxiom.pipeline.Credential

trait GCPCredential extends Credential {
  override def name: String = "GCPCredential"
  def authKey: Array[Byte]
}

/**
  * GCPCredential implementation that loads the authorization json key from a Base64 string. It is not
  * recommended that this be used in production projects.
  * @param parameters Startup parameters
  */
class Base64GCPCredential(override val parameters: Map[String, Any]) extends GCPCredential {
  private val authKeyArray = Base64.getDecoder.decode(parameters("gcpAuthKey").asInstanceOf[String])

  override def authKey: Array[Byte] = authKeyArray
}
