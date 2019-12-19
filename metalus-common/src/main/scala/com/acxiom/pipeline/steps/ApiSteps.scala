package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.StepFunction
import com.acxiom.pipeline.api.{Authorization, HttpRestClient}

object ApiSteps {
  @StepFunction("15889487-fd1c-4c44-b8eb-973c12f91fae",
    "Creates an HttpRestClient",
    "This step will build an HttpRestClient using a host url and optional authorization object",
    "Pipeline",
    "API")
  def createHttpRestClient(hostUrl: String, authorization: Option[Authorization] = None): HttpRestClient = {
    new HttpRestClient(hostUrl, authorization)
  }

  @StepFunction("fcfd4b91-9a9c-438c-8afa-9f14c1e52a82",
    "Creates an HttpRestClient from protocol, host and port",
    "This step will build an HttpRestClient using url parts and optional authorization object",
    "Pipeline",
    "API")
  def createHttpRestClientFromParameters(protocol: String,
                           host: String,
                           port: Int,
                           authorization: Option[Authorization] = None): HttpRestClient = {
    createHttpRestClient(s"$protocol://$host:$port", authorization)
  }
}
