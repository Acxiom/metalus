package com.acxiom.pipeline

import com.acxiom.pipeline.drivers.DriverSetup

object MockStepObject {
  def mockStepFunction(string: String, boolean: Boolean): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("boolean" -> boolean)))
  }

  def mockStepFunction(string: String, boolean: Boolean, opt: Option[String]): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("boolean" -> boolean, "option" -> opt)))
  }

  def mockStepFunctionAnyResponse(string: String): String = {
    string
  }
}

case class MockClass(string: String)

class MockNoParams {
  def string: String = "no-constructor-string"
}

case class MockDriverSetup(parameters: Map[String, Any]) extends DriverSetup {
  override def pipelines: List[Pipeline] = List()

  override def initialPipelineId: String = {
    if (parameters.contains("initialPipelineId")) {
      parameters("initialPipelineId").asInstanceOf[String]
    } else {
      ""
    }
  }

  override def pipelineContext: PipelineContext = {
    PipelineContext(None, None, None, PipelineSecurityManager(), PipelineParameters(),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      } else {
        List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")
      }),
      PipelineStepMapper(),
      None, None)
  }
}
