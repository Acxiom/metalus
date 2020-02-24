package com.acxiom.pipeline.api

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL}

import org.json4s.{DefaultFormats, Formats}

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
  def exists(path: String): Boolean = {
    this.openUrlConnection(path).getResponseCode != 404
  }

  /**
    * Creates a buffered input stream for the provided path.
    *
    * @param path       The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @return A buffered input stream
    */
  def getInputStream(path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): InputStream = {
    new BufferedInputStream(this.openUrlConnection(path).getInputStream, bufferSize)
  }

  /**
    * Creates a buffered output stream for the provided path.
    *
    * @param path       The path where data will be written.
    * @param bufferSize The buffer size to apply to the stream
    * @return
    */
  def getOutputStream(path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): OutputStream = {
    val connection = this.openUrlConnection(path)
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "multipart/form-data")
    new BufferedOutputStream(connection.getOutputStream, bufferSize)
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
    * @param contentType The content type to post. Defaults to JSON.
    * @return The String output of the command.
    */
  def postJsonContent(path: String, body: String, contentType: String = "application/json"): String = {
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
  def putJsonContent(path: String, body: String, contentType: String = "application/json"): String = {
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

  /**
    * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
    *
    * @param path The path to the file
    * @return size of the given file
    */
  def getContentLength(path: String): Long = {
    this.openUrlConnection(path).getContentLength
  }

  private def openUrlConnection(path: String): HttpURLConnection = {
    val connection = new URL(baseUrl, path).openConnection().asInstanceOf[HttpURLConnection]
    if (authorization.isDefined) {
      authorization.get.authorize(connection)
    }
    connection
  }
}
