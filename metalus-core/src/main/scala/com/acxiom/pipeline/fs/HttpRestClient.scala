package com.acxiom.pipeline.fs

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL}

import org.json4s.{DefaultFormats, Formats}

import scala.io.Source

object HttpRestClient {
  val DEFAULT_BUFFER_SIZE: Int = 65536

  def apply(hostUrl: String): HttpRestClient = new HttpRestClient(hostUrl)

  def apply(protocol: String, host: String, port: Int = HttpRestClient.DEFAULT_PORT): HttpRestClient =
    new HttpRestClient(protocol, host, port)

  val DEFAULT_PORT = 80
}

class HttpRestClient(hostUrl: String) {

  private implicit val formats: Formats = DefaultFormats

  def this(protocol: String, host: String, port: Int = HttpRestClient.DEFAULT_PORT) {
    this(s"$protocol://$host:$port")
  }

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
    val contentType = new URL(baseUrl, path).openConnection().getContentType
    Option(contentType).isDefined
  }

  /**
    * Creates a buffered input stream for the provided path.
    *
    * @param path       The path to read data from
    * @param bufferSize The buffer size to apply to the stream
    * @return A buffered input stream
    */
  def getInputStream(path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): InputStream = {
    new BufferedInputStream(new URL(baseUrl, path).openConnection().getInputStream, bufferSize)
  }

  /**
    * Creates a buffered output stream for the provided path.
    *
    * @param path       The path where data will be written.
    * @param bufferSize The buffer size to apply to the stream
    * @return
    */
  def getOutputStream(path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): OutputStream = {
    val connection = new URL(baseUrl, path).openConnection().asInstanceOf[HttpURLConnection]
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
    * Attempts to delete the provided path.
    *
    * @param path The path to delete.
    * @return True if the path could be deleted.
    */
  def delete(path: String): Boolean = {
    val connection = new URL(baseUrl, path).openConnection().asInstanceOf[HttpURLConnection]
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
    new URL(baseUrl, path).openConnection().getContentLength
  }
}
