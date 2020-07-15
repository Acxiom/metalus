package com.acxiom.pipeline.api

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.util.Date

import com.acxiom.pipeline.Constants
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConverters._
import scala.io.Source

object HttpRestClient {
  val DEFAULT_BUFFER_SIZE: Int = 65536

  def apply(hostUrl: String): HttpRestClient = new HttpRestClient(hostUrl)

  def apply(hostUrl: String, authorization: Authorization): HttpRestClient = new HttpRestClient(hostUrl, authorization)

  def apply(protocol: String, host: String, port: Int = HttpRestClient.DEFAULT_PORT): HttpRestClient =
    new HttpRestClient(protocol, host, port)

  def apply(protocol: String, host: String, port: Int, authorization: Authorization): HttpRestClient =
    new HttpRestClient(protocol, host, port, authorization)

  val DEFAULT_PORT = 80
}

class HttpRestClient(hostUrl: String, authorization: Option[Authorization]) {
  def this(hostUrl: String) = {
    this(hostUrl, None)
  }

  def this(hostUrl: String, authorization: Authorization) = {
    this(hostUrl, Some(authorization))
  }

  def this(protocol: String, host: String, port: Int = HttpRestClient.DEFAULT_PORT) {
    this(s"$protocol://$host:$port")
  }

  def this(protocol: String, host: String, port: Int, authorization: Authorization) {
    this(s"$protocol://$host:$port", authorization)
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
  def exists(path: String, headers: Option[Map[String, String]] = None): Boolean = {
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
  def getContentLength(path: String, headers: Option[Map[String, String]] = None): Long = {
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
  def getLastModifiedDate(path: String, headers: Option[Map[String, String]] = None): Date = {
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
                     bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE,
                     headers: Option[Map[String, String]] = None): InputStream = {
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
                      bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE,
                      headers: Option[Map[String, String]] = None): OutputStream = {
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
  def getStringContent(path: String, headers: Option[Map[String, String]] = None): String = {
    val input = getInputStream(path, headers = headers)
    val content = Source.fromInputStream(input).mkString
    input.close()
    content
  }

  /**
    * Simple method to post string content.
    *
    * @param path        The path to post the content
    * @param body        The body to post
    * @return The String output of the command.
    */
  def postJsonContent(path: String, body: String, headers: Option[Map[String, String]] = None): String = {
    upsertJsonContent(path, body, "POST", Constants.JSON_CONTENT_TYPE, headers)
  }

  /**
    * Simple method to put string content.
    *
    * @param path        The path to put the content
    * @param body        The body to put
    * @return The String output of the command.
    */
  def putJsonContent(path: String,body: String, headers: Option[Map[String, String]] = None): String = {
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
                        contentType: String = Constants.JSON_CONTENT_TYPE,
                        headers: Option[Map[String, String]] = None): String = {
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
                       contentType: String = Constants.JSON_CONTENT_TYPE,
                       headers: Option[Map[String, String]] = None): String = {
    upsertJsonContent(path, body, "PUT", contentType, headers)
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
    val output = connection.getOutputStream
    output.write(body.getBytes, 0, body.length)
    output.flush()
    output.close()
    val input = connection.getInputStream
    val content = Source.fromInputStream(input).mkString
    input.close()
    connection.disconnect()
    content
  }

  /**
    * Attempts to delete the provided path.
    *
    * @param path The path to delete.
    * @return True if the path could be deleted.
    */
  def delete(path: String, headers: Option[Map[String, String]] = None): Boolean = {
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
