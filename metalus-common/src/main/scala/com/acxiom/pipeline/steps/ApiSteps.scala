package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.api.{Authorization, HttpRestClient}

import java.io.{InputStream, OutputStream}
import java.util.Date

@StepObject
object ApiSteps {
  @StepFunction("15889487-fd1c-4c44-b8eb-973c12f91fae",
    "Creates an HttpRestClient",
    "This step will build an HttpRestClient using a host url and optional authorization object",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "hostUrl" -> StepParameter(None, Some(true), None, None, None, None, Some("The URL to connect including port")),
    "authorization" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional authorization class to use when making connections")),
    "allowSelfSignedCertificates" ->
      StepParameter(None, Some(false), None, None, None, None, Some("Flag to allow using self signed certificates for http calls"))))
  @StepResults(primaryType = "com.acxiom.pipeline.api.HttpRestClient", secondaryTypes = None)
  def createHttpRestClient(hostUrl: String,
                           authorization: Option[Authorization] = None,
                           allowSelfSignedCertificates: Boolean = false): HttpRestClient = {
    new HttpRestClient(hostUrl, authorization, allowSelfSignedCertificates)
  }

  @StepFunction("fcfd4b91-9a9c-438c-8afa-9f14c1e52a82",
    "Creates an HttpRestClient from protocol, host and port",
    "This step will build an HttpRestClient using url parts and optional authorization object",
    "Pipeline",
    "API")
  @StepParameters(Map("protocol" -> StepParameter(None, Some(true), None, None, None, None, Some("The protocol to use when constructing the URL")),
    "host" -> StepParameter(None, Some(true), None, None, None, None, Some("The host name to use when constructing the URL")),
    "port" -> StepParameter(None, Some(true), None, None, None, None, Some("The port to use when constructing the URL")),
    "authorization" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional authorization class to use when making connections"))))
  @StepResults(primaryType = "com.acxiom.pipeline.api.HttpRestClient", secondaryTypes = None)
  def createHttpRestClientFromParameters(protocol: String,
                                         host: String,
                                         port: Int,
                                         authorization: Option[Authorization] = None): HttpRestClient = {
    createHttpRestClient(s"$protocol://$host:$port", authorization)
  }

  @StepFunction("b59f0486-78aa-4bd4-baf5-5c7d7c648ff0",
    "Check Path Exists",
    "Checks the path to determine whether it exists or not.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to verify"))))
  @StepResults(primaryType = "Boolean", secondaryTypes = None)
  def exists(httpRestClient: HttpRestClient, path: String): Boolean = {
    httpRestClient.exists(path)
  }

  @StepFunction("7521ac47-84ec-4e50-b087-b9de4bf6d514",
    "Get the last modified date",
    "Gets the last modified date for the provided path",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to the resource to get the last modified date"))))
  @StepResults(primaryType = "java.util.Date", secondaryTypes = None)
  def getLastModifiedDate(httpRestClient: HttpRestClient, path: String): Date = {
    httpRestClient.getLastModifiedDate(path)
  }

  @StepFunction("fff7f7b6-5d9a-40b3-8add-6432552920a8",
    "Get Path Content Length",
    "Get the size of the content at the given path.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to the resource to get the content length"))))
  @StepResults(primaryType = "Long", secondaryTypes = None)
  def getContentLength(httpRestClient: HttpRestClient, path: String): Long = {
    httpRestClient.getContentLength(path)
  }

  @StepFunction("dd351d47-125d-47fa-bafd-203bebad82eb",
    "Get Path Headers",
    "Get the headers for the content at the given path.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to get the headers"))))
  @StepResults(primaryType = "Map[String,List[String]]", secondaryTypes = None)
  def getHeaders(httpRestClient: HttpRestClient, path: String): Map[String, List[String]] = {
    httpRestClient.getHeaders(path)
  }

  @StepFunction("532f72dd-8443-481d-8406-b74cdc08e342",
    "Delete Content",
    "Attempts to delete the provided path..",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to delete"))))
  @StepResults(primaryType = "Boolean", secondaryTypes = None)
  def delete(httpRestClient: HttpRestClient, path: String): Boolean = {
    httpRestClient.delete(path)
  }

  @StepFunction("3b91e6e8-ec18-4468-9089-8474f4b4ba48",
    "GET String Content",
    "Retrieves the value at the provided path as a string.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to resource"))))
  @StepResults(primaryType = "String", secondaryTypes = None)
  def getStringContent(httpRestClient: HttpRestClient, path: String): String = {
    httpRestClient.getStringContent(path)
  }

  @StepFunction("34c2fc9a-2502-4c79-a0cb-3f866a0a0d6e",
    "POST String Content",
    "POSTs the provided string to the provided path using the content type and returns the response as a string.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to post the content")),
    "content" -> StepParameter(None, Some(true), None, None, None, None, Some("The content to post")),
    "contentType" -> StepParameter(None, Some(false), None, None, None, None, Some("The content type being sent to the path"))))
  @StepResults(primaryType = "String", secondaryTypes = None)
  def postStringContent(httpRestClient: HttpRestClient, path: String, content: String, contentType: String = "application/json"): String = {
    httpRestClient.postStringContent(path, content, contentType)
  }

  @StepFunction("49ae38b3-cb41-4153-9111-aa6aacf6721d",
    "PUT String Content",
    "PUTs the provided string to the provided path using the content type and returns the response as a string.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to post the content")),
    "content" -> StepParameter(None, Some(true), None, None, None, None, Some("The content to put")),
    "contentType" -> StepParameter(None, Some(false), None, None, None, None, Some("The content type being sent to the path"))))
  @StepResults(primaryType = "String", secondaryTypes = None)
  def putStringContent(httpRestClient: HttpRestClient, path: String, content: String, contentType: String = "application/json"): String = {
    httpRestClient.putStringContent(path, content, contentType)
  }

  @StepFunction("99b20c23-722f-4862-9f47-bc9f72440ae6",
    "GET Input Stream",
    "Creates a buffered input stream for the provided path",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to the resource")),
    "bufferSize" -> StepParameter(None, Some(false), None, None, None, None, Some("The size of buffer to use with the stream"))))
  @StepResults(primaryType = "java.io.InputStream", secondaryTypes = None)
  def getInputStream(httpRestClient: HttpRestClient, path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): InputStream = {
    httpRestClient.getInputStream(path, bufferSize)
  }

  @StepFunction("f4120b1c-91df-452f-9589-b77f8555ba44",
    "GET Output Stream",
    "Creates a buffered output stream for the provided path.",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "httpRestClient" -> StepParameter(None, Some(true), None, None, None, None, Some("The HttpRestClient to use when accessing the provided path")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The path to the resource")),
    "bufferSize" -> StepParameter(None, Some(false), None, None, None, None, Some("The size of buffer to use with the stream"))))
  @StepResults(primaryType = "java.io.OutputStream", secondaryTypes = None)
  def getOutputStream(httpRestClient: HttpRestClient, path: String, bufferSize: Int = HttpRestClient.DEFAULT_BUFFER_SIZE): OutputStream = {
    httpRestClient.getOutputStream(path, bufferSize)
  }
}
