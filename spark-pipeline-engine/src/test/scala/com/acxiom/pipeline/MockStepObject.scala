package com.acxiom.pipeline

import com.acxiom.pipeline.drivers.DriverSetup

object MockStepObject {
  def mockStepFunction(string: String, boolean: Boolean): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("boolean" -> boolean, "string" -> string)))
  }

  def mockStepFunction(string: String, boolean: Boolean, opt: Option[String]): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("boolean" -> boolean, "string" -> string, "option" -> opt)))
  }

  def mockStepFunctionWithDefaultValue(string: String, default: Option[String] = Some("chicken")): Option[String] = {
    default
  }

  def mockStepFunctionAnyResponse(string: String): String = {
    string
  }

  def mockStepFunctionWithOptionalGenericParams(list: Option[Seq[String]]): String ={
    list.getOrElse(List("chicken")).headOption.getOrElse("chicken")
  }

  def mockStepFunctionWithPrimitives(i: Int, l: Long, d: Double, f: Float, c: Char, by: Option[Byte], s: Short, a: Any): Int ={
    i
  }

  def mockStringListStepFunction(listSize: Int): PipelineStepResponse = {
    PipelineStepResponse(Some(List.tabulate(listSize)(_.toString)), None)
  }

  def mockExceptionStepFunction(string: String): PipelineStepResponse = {
    throw new IllegalArgumentException(s"exception thrown for string value ($string)")
  }

  def mockStepSetGlobal(string: String, globalName: String): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("$globals." + globalName -> string)))
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
