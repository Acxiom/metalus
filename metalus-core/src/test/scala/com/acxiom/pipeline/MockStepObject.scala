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

  def mockStepFunctionWithDefaultValueNoOption(string: String, default: String = "default chicken"): String = {
    default
  }

  def mockStepWithListOfOptions(s: List[Option[String]]): String ={
    s.flatten.mkString(",")
  }

  def mockStepFunctionWithListParams(list: List[String], seq: Seq[Int], arrayList: java.util.ArrayList[String]): String ={
    s"${list.headOption},${seq.headOption},${if(arrayList.isEmpty) None else Some(arrayList.get(0))}"
  }

  def mockStepFunctionAnyResponse(string: String): String = {
    s"string: $string"
  }

  def mockStepFunctionWithOptionalGenericParams(string: Option[String]): String ={
    string.getOrElse("chicken")
  }

  def mockStepFunctionWithPrimitives(i: Int, l: Long, d: Double, f: Float, c: Char, by: Option[Byte], s: Short, a: Any): Int ={
    i
  }

  def mockStepFunctionWithBoxClasses(i: Integer, l: java.lang.Long, d: java.lang.Double, f: java.lang.Float, c: Character,
                                     by: java.lang.Byte, s: java.lang.Short): Int = {
    i
  }

  def mockStringListStepFunction(listSize: Int): PipelineStepResponse = {
    PipelineStepResponse(Some(List.tabulate(listSize)(_.toString)), None)
  }

  def mockExceptionStepFunction(string: String): PipelineStepResponse = {
    throw new IllegalArgumentException(s"exception thrown for string value ($string)")
  }

  def errorHandlingStep(ex: PipelineStepException): String = {
    ex.message.getOrElse("")
  }

  def mockStepSetGlobal(string: String, globalName: String): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("$globals." + globalName -> string)))
  }
  def mockStepSetMetric(string: String, metricName: String): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any](s"$$metrics.$metricName" -> string)))
  }
}

case class MockClass(string: String)

class MockNoParams {
  def string: String = "no-constructor-string"
}

class MockDefaultParam(flag: Boolean = false, secondParam: String = "none") {
  def getFlag: Boolean = flag
  def getSecondParam: String = secondParam
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
