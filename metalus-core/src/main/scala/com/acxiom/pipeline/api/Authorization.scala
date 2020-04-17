package com.acxiom.pipeline.api

import java.net.{HttpURLConnection, URL, URLConnection}
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

case class BasicAuthorization(username: String, password: String) extends Authorization {
  private val userPass = Base64.getEncoder.encodeToString(s"$username:$password".getBytes)
  /**
    * Performs basic authorization against the provided URLConnection.
    * @param urlConnection The URLConnection to authorize.
    */
  override def authorize(urlConnection: URLConnection): Unit = {
    urlConnection.setRequestProperty("Authorization", s"Basic $userPass")
  }
}

case class SessionAuthorization(username: String, password: String, authUrl: String) extends Authorization {
  private lazy val authHeader = {
    val body =
      s"""{
         |"username": "$username",
         |"password": "$password"
         |}""".stripMargin
    val connection = new URL(authUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setRequestMethod("POST")
    connection.setDoOutput(true)
    val output = connection.getOutputStream
    output.write(body.getBytes, 0, body.length)
    output.flush()
    output.close()
    val connId = connection.getHeaderField("Set-Cookie")
    connection.disconnect()
    connId
  }

  /**
    * Performs authorization against the provided URLConnection.
    *
    * @param urlConnection The URLConnection to authorize.
    */
  override def authorize(urlConnection: URLConnection): Unit = {
    urlConnection.setRequestProperty("Cookie", authHeader)
  }
}
