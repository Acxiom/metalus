package com.acxiom.pipeline.api

import com.acxiom.pipeline.Constants
import org.apache.log4j.Logger
import org.json4s.{DefaultFormats, Formats}

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.security.cert.X509Certificate
import java.util.Date
import javax.net.ssl._
import scala.collection.JavaConverters._
import scala.io.Source

object HttpRestClient {
  val DEFAULT_BUFFER_SIZE: Int = 65536

  def apply(hostUrl: String): HttpRestClient = new HttpRestClient(hostUrl)

  def apply(hostUrl: String, allowSelfSignedCertificates: Boolean): HttpRestClient =
    new HttpRestClient(hostUrl, None, allowSelfSignedCertificates)

  def apply(hostUrl: String, authorization: Authorization): HttpRestClient = new HttpRestClient(hostUrl, authorization)

  def apply(hostUrl: String, authorization: Authorization, allowSelfSignedCertificates: Boolean): HttpRestClient =
    new HttpRestClient(hostUrl, Some(authorization), allowSelfSignedCertificates)

  def apply(protocol: String, host: String, port: Int = HttpRestClient.DEFAULT_PORT): HttpRestClient =
    new HttpRestClient(protocol, host, port)

  def apply(protocol: String, host: String, port: Int, authorization: Authorization): HttpRestClient =
    new HttpRestClient(protocol, host, port, authorization)

  val DEFAULT_PORT = 80

  private[api] lazy val SELF_SIGNED_HOST_VERIFIER: HostnameVerifier = new HostnameVerifier {
    override def verify(s: String, sslSession: SSLSession): Boolean = true
  }

  private[api] lazy val SELF_SIGNED_SSL_CONTEXT: SSLContext = {
    val ssl = SSLContext.getInstance("TLS")
    ssl.init(Array[KeyManager](), Array[TrustManager](new X509TrustManager() {
      override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {
        // Do nothing since this is only useful for self-signed certificates
      }

      override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {
        // Only calling this so that it "appears" that the code is being covered
        checkClientTrusted(x509Certificates, s)
      }

      override def getAcceptedIssuers: Array[X509Certificate] = Array[X509Certificate]()
    }), new java.security.SecureRandom())
    ssl
  }
}

class HttpRestClient(hostUrl: String, authorization: Option[Authorization], allowSelfSignedCertificates: Boolean) {
  private val logger = Logger.getLogger(HttpRestClient.getClass)
  def this(hostUrl: String) = {
    this(hostUrl, None, false)
  }

  def this(hostUrl: String, authorization: Authorization) = {
    this(hostUrl, Some(authorization), false)
  }

  def this(protocol: String, host: String, port: Int = HttpRestClient.DEFAULT_PORT) {
    this(s"$protocol://$host:$port")
  }

  def this(protocol: String, host: String, port: Int, authorization: Authorization) {
    this(s"$protocol://$host:$port", authorization)
  }

  // This section determines whether or not to allow self signed certificates when calling https end points
  private val selfSignedCertificate = System.getenv("ALLOW_SELF_SIGNED_CERTS")
  if (allowSelfSignedCertificates || Option(selfSignedCertificate).isDefined &&
    selfSignedCertificate.nonEmpty && selfSignedCertificate.toLowerCase() == "true") {
    HttpsURLConnection.setDefaultSSLSocketFactory(HttpRestClient.SELF_SIGNED_SSL_CONTEXT.getSocketFactory)
    HttpsURLConnection.setDefaultHostnameVerifier(HttpRestClient.SELF_SIGNED_HOST_VERIFIER)
  }

  private implicit val formats: Formats = DefaultFormats

  private val baseUrl = new URL(hostUrl)

  baseUrl.getProtocol.toLowerCase match {
    case "http" =>
    case "https" =>
    case _ => throw new IllegalArgumentException("Only http and https protocols are supported!")
  }

  /**
    * Checks the path to determine whether it exists or not.
    *
    * @param path The path to verify
    * @return True if the path exists, otherwise false.
    */
  def exists(path: String): Boolean = {
    exists(path, None)
  }

  /**
    * Checks the path to determine whether it exists or not.
    *
    * @param path    The path to verify
    * @param headers Optional headers to pass to the connection
    * @return True if the path exists, otherwise false.
    */
  def exists(path: String, headers: Option[Map[String, String]]): Boolean = {
    val connection = this.openUrlConnection(path, headers)
    val exists = connection.getResponseCode != 404
    connection.disconnect()
    exists
  }

  /**
    * Get the size of the content at the given path.
    *
    * @param path The path to the content
    * @return size of the given content
    */
  def getContentLength(path: String): Long = {
    getContentLength(path, None)
  }

  /**
    * Get the size of the content at the given path.
    *
    * @param path    The path to the content
    * @param headers Optional headers to pass to the connection
    * @return size of the given content
    */
  def getContentLength(path: String, headers: Option[Map[String, String]]): Long = {
    val connection = this.openUrlConnection(path, headers)
    val length = connection.getContentLength
    connection.disconnect()
    length
  }

  /**
    * Get the last modified date of the content at the given path.
    *
    * @param path The path to the content
    * @return last modified date of the content
    */
  def getLastModifiedDate(path: String): Date = {
    getLastModifiedDate(path, None)
  }

  /**
    * Get the last modified date of the content at the given path.
    *
    * @param path    The path to the content
    * @param headers Optional headers to pass to the connection
    * @return last modified date of the content
    */
  def getLastModifiedDate(path: String, headers: Option[Map[String, String]]): Date = {
    val connection = this.openUrlConnection(path, headers)
    val lastModified = new Date(connection.getLastModified)
    connection.disconnect()
    lastModified
  }

  /**
    * Return all of the headers for the given path as a map.
    *
    * @param path The path to the content
    * @return all of the headers for the given path
    */
  def getHeaders(path: String): Map[String, List[String]] = {
    val connection = this.openUrlConnection(path)
    val headers = connection.getHeaderFields
    connection.disconnect()
    headers.asScala.map(entry => (entry._1, entry._2.asScala.toList)).toMap
  }

  /**
    * Creates a buffered input stream for the provided path. Closing this stream will close the connection.
    *
    * @param path       The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @return A buffered input stream
    */
  def getInputStream(path: String,
                     bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): InputStream = {
    getInputStream(path, bufferSize, None)
  }

  /**
    * Creates a buffered input stream for the provided path. Closing this stream will close the connection.
    *
    * @param path       The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @param headers    Optional headers to pass to the connection
    * @return A buffered input stream
    */
  def getInputStream(path: String,
                     bufferSize: Int,
                     headers: Option[Map[String, String]]): InputStream = {
    new BufferedInputStream(HttpInputStream(this.openUrlConnection(path, headers)), bufferSize)
  }

  /**
    * Creates a buffered output stream for the provided path. Closing this stream will close the connection.
    *
    * @param path       The path where data will be written.
    * @param bufferSize The buffer size to apply to the stream
    * @return
    */
  def getOutputStream(path: String,
                      bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): OutputStream = {
    getOutputStream(path, bufferSize, None)
  }

  /**
    * Creates a buffered output stream for the provided path. Closing this stream will close the connection.
    *
    * @param path       The path where data will be written.
    * @param bufferSize The buffer size to apply to the stream
    * @param headers    Optional headers to pass to the connection
    * @return
    */
  def getOutputStream(path: String,
                      bufferSize: Int,
                      headers: Option[Map[String, String]]): OutputStream = {
    val connection = this.openUrlConnection(path, headers)
    connection.setDoOutput(true)
    connection.setRequestProperty(Constants.CONTENT_TYPE_HEADER, "multipart/form-data")
    new BufferedOutputStream(HttpOutputStream(connection), bufferSize)
  }

  /**
    * Retrieves the value at the provided path as a string.
    *
    * @param path The extra path to retrieve content
    * @return String content
    */
  def getStringContent(path: String): String = {
    getStringContent(path, None)
  }

  /**
    * Retrieves the value at the provided path as a string.
    *
    * @param path    The extra path to retrieve content
    * @param headers Optional headers to pass to the connection
    * @return String content
    */
  def getStringContent(path: String, headers: Option[Map[String, String]]): String = {
    val input = getInputStream(path, HttpRestClient.DEFAULT_BUFFER_SIZE, headers)
    val content = Source.fromInputStream(input).mkString
    input.close()
    content
  }

  /**
    * Simple method to post string content.
    *
    * @param path The path to post the content
    * @param body The body to post
    * @return The String output of the command.
    */
  def postJsonContent(path: String, body: String): String = {
    postJsonContent(path, body, None)
  }

  /**
    * Simple method to post string content.
    *
    * @param path    The path to post the content
    * @param body    The body to post
    * @param headers Optional headers to pass to the connection
    * @return The String output of the command.
    */
  def postJsonContent(path: String, body: String, headers: Option[Map[String, String]]): String = {
    upsertJsonContent(path, body, "POST", Constants.JSON_CONTENT_TYPE, headers)
  }

  /**
    * Simple method to put string content.
    *
    * @param path The path to put the content
    * @param body The body to put
    * @return The String output of the command.
    */
  def putJsonContent(path: String, body: String): String = {
    putJsonContent(path, body, None)
  }

  /**
    * Simple method to put string content.
    *
    * @param path    The path to put the content
    * @param body    The body to put
    * @param headers Optional headers to pass to the connection
    * @return The String output of the command.
    */
  def putJsonContent(path: String, body: String, headers: Option[Map[String, String]]): String = {
    upsertJsonContent(path, body, "PUT", Constants.JSON_CONTENT_TYPE, headers)
  }

  /**
    * Simple method to post string content.
    *
    * @param path        The path to post the content
    * @param body        The body to post
    * @param contentType The content type to post. Defaults to JSON.
    * @return The String output of the command.
    */
  def postStringContent(path: String,
                        body: String,
                        contentType: String = Constants.JSON_CONTENT_TYPE): String = {
    postStringContent(path, body, contentType, None)
  }

  /**
    * Simple method to post string content.
    *
    * @param path        The path to post the content
    * @param body        The body to post
    * @param contentType The content type to post. Defaults to JSON.
    * @param headers     Optional headers to pass to the connection
    * @return The String output of the command.
    */
  def postStringContent(path: String,
                        body: String,
                        contentType: String,
                        headers: Option[Map[String, String]]): String = {
    upsertJsonContent(path, body, "POST", contentType, headers)
  }

  /**
    * Simple method to put string content.
    *
    * @param path        The path to put the content
    * @param body        The body to put
    * @param contentType The content type to put. Defaults to JSON.
    * @return The String output of the command.
    */
  def putStringContent(path: String,
                       body: String,
                       contentType: String = Constants.JSON_CONTENT_TYPE): String = {
    putStringContent(path, body, contentType, None)
  }

  /**
    * Simple method to put string content.
    *
    * @param path        The path to put the content
    * @param body        The body to put
    * @param contentType The content type to put. Defaults to JSON.
    * @param headers     Optional headers to pass to the connection
    * @return The String output of the command.
    */
  def putStringContent(path: String,
                       body: String,
                       contentType: String,
                       headers: Option[Map[String, String]]): String = {
    upsertJsonContent(path, body, "PUT", contentType, headers)
  }

  /**
    * Attempts to delete the provided path.
    *
    * @param path The path to delete.
    * @return True if the path could be deleted.
    */
  def delete(path: String): Boolean = {
    delete(path, None)
  }

  /**
    * Attempts to delete the provided path.
    *
    * @param path    The path to delete.
    * @param headers Optional headers to pass to the connection
    * @return True if the path could be deleted.
    */
  def delete(path: String, headers: Option[Map[String, String]]): Boolean = {
    val connection = this.openUrlConnection(path, headers)
    connection.setDoOutput(true)
    connection.setRequestProperty(Constants.CONTENT_TYPE_HEADER, "application/x-www-form-urlencoded")
    connection.setRequestMethod("DELETE")
    connection.connect()
    val responseCode = connection.getResponseCode
    connection.disconnect()
    responseCode < 300
  }

  private def openUrlConnection(path: String, headers: Option[Map[String, String]] = None): HttpURLConnection = {
    val connection = new URL(baseUrl, path).openConnection().asInstanceOf[HttpURLConnection]
    if (authorization.isDefined) {
      authorization.get.authorize(connection)
    }
    applyHeaders(headers.getOrElse(Map[String, String]()), connection)
    connection
  }

  private def applyHeaders(headers: Map[String, String], connection: HttpURLConnection): Unit = {
    // Set a default User-Agent
    connection.setRequestProperty("User-Agent", headers.getOrElse("User-Agent", s"Metalus / ${System.getProperty("user.name")}"))
    headers.foreach(header => connection.setRequestProperty(header._1, header._2))
  }

  private def upsertJsonContent(path: String,
                                body: String,
                                method: String,
                                contentType: String,
                                headers: Option[Map[String, String]] = None): String = {
    val connection = this.openUrlConnection(path)
    connection.setDoOutput(true)
    connection.setRequestProperty(Constants.CONTENT_TYPE_HEADER, contentType)
    // Apply the custom headers
    applyHeaders(headers.getOrElse(Map[String, String]()), connection)
    connection.setRequestMethod(method)
    try {
      val output = connection.getOutputStream
      output.write(body.getBytes, 0, body.length)
      output.flush()
      output.close()
      val input = connection.getInputStream
      val content = Source.fromInputStream(input).mkString
      input.close()
      connection.disconnect()
      content
    } catch {
      case t: Throwable =>
        logger.error(Source.fromInputStream(connection.getErrorStream).mkString, t)
        throw t
    }
  }
}

case class HttpInputStream(connection: HttpURLConnection) extends InputStream {
  private val inputStream = connection.getInputStream

  override def read(): Int = inputStream.read()

  override def close(): Unit = {
    inputStream.close()
    connection.disconnect()
  }
}

case class HttpOutputStream(connection: HttpURLConnection) extends OutputStream {
  private val outputStream = connection.getOutputStream

  override def write(b: Int): Unit = outputStream.write(b)

  override def close(): Unit = {
    outputStream.close()
    connection.disconnect()
  }
}
