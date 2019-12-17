package com.acxiom.pipeline.api

import java.net.URLConnection
import java.util.Base64

/**
  * Provides a basic mechanism for authorizing a single URLConnection.
  */
trait Authorization {
  /**
    * Performs authorization against the provided URLConnection.
    * @param urlConnection The URLConnection to authorize.
    */
  def authorize(urlConnection: URLConnection): Unit
}

class BasicAuthorization(username: String, password: String) extends Authorization {
  private val userPass = Base64.getEncoder.encodeToString(s"$username:$password".getBytes)
  /**
    * Performs basic authorization against the provided URLConnection.
    * @param urlConnection The URLConnection to authorize.
    */
  override def authorize(urlConnection: URLConnection): Unit = {
    urlConnection.setRequestProperty("Authorization", s"Basic $userPass")
  }
}
