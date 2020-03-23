package com.acxiom.pipeline.api

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.util.Date

import org.json4s.{DefaultFormats, Formats}

import scala.io.Source
import scala.collection.JavaConverters._

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
  def exists(path: String): Boolean = {
    val connection = this.openUrlConnection(path)
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
    val connection = this.openUrlConnection(path)
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
    val connection = this.openUrlConnection(path)
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
  def getInputStream(path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): InputStream = {
    new BufferedInputStream(HttpInputStream(this.openUrlConnection(path)), bufferSize)
  }

  /**
    * Creates a buffered output stream for the provided path. Closing this stream will close the connection.
    *
    * @param path       The path where data will be written.
    * @param bufferSize The buffer size to apply to the stream
    * @return
    */
  def getOutputStream(path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): OutputStream = {
    val connection = this.openUrlConnection(path)
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "multipart/form-data")
    new BufferedOutputStream(HttpOutputStream(connection), bufferSize)
  }

  /**
    * Retrieves the value at the provided path as a string.
    *
    * @param path The extra path to retrieve content
    * @return String content
    */
  def getStringContent(path: String): String = {
    val input = getInputStream(path)
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
  def postJsonContent(path: String, body: String): String = {
    upsertJsonContent(path, body, "POST", "application/json")
  }

  /**
    * Simple method to put string content.
    *
    * @param path        The path to put the content
    * @param body        The body to put
    * @return The String output of the command.
    */
  def putJsonContent(path: String, body: String): String = {
    upsertJsonContent(path, body, "PUT", "application/json")
  }

  /**
    * Simple method to post string content.
    *
    * @param path        The path to post the content
    * @param body        The body to post
    * @param contentType The content type to post. Defaults to JSON.
    * @return The String output of the command.
    */
  def postStringContent(path: String, body: String, contentType: String = "application/json"): String = {
    upsertJsonContent(path, body, "POST", contentType)
  }

  /**
    * Simple method to put string content.
    *
    * @param path        The path to put the content
    * @param body        The body to put
    * @param contentType The content type to put. Defaults to JSON.
    * @return The String output of the command.
    */
  def putStringContent(path: String, body: String, contentType: String = "application/json"): String = {
    upsertJsonContent(path, body, "PUT", contentType)
  }

  private def upsertJsonContent(path: String, body: String, method: String, contentType: String): String = {
    val connection = this.openUrlConnection(path)
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", contentType)
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
  def delete(path: String): Boolean = {
    val connection = this.openUrlConnection(path)
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    connection.setRequestMethod("DELETE")
    connection.connect()
    val responseCode = connection.getResponseCode
    connection.disconnect()
    responseCode < 300
  }

  private def openUrlConnection(path: String): HttpURLConnection = {
    val connection = new URL(baseUrl, path).openConnection().asInstanceOf[HttpURLConnection]
    if (authorization.isDefined) {
      authorization.get.authorize(connection)
    }
    connection
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
