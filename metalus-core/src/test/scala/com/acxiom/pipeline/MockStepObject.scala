package com.acxiom.pipeline

import com.acxiom.pipeline.drivers.DriverSetup

object MockStepObject {
  def mockStepFunction(string: String, boolean: Boolean): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("boolean" -> boolean, "string" -> string)))
  }

  def mockStepFunction(string: String, boolean: Boolean, opt: Option[String]): PipelineStepResponse = {
    PipelineStepResponse(Some(string), Some(Map[String, Any]("boolean" -> boolean, "string" -> string, "option" -> opt)))
  }

  def mockStepGlobalsUpdateFunction(string: String, boolean: Boolean, global: Option[String]): PipelineStepResponse = {
    val secondaryMap = if (global.isDefined) {
      Map[String, Any]("boolean" -> boolean, "string" -> string,
        "$globals.mockGlobal" -> global.get,
        "$globalLink.mockGlobalLink" -> "!some global link")
    } else {
      Map[String, Any]("boolean" -> boolean, "string" -> string)
    }
    PipelineStepResponse(Some(string), Some(secondaryMap))
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

  def mockFlattenListOfOptions(s: List[Option[String]]): List[String] = s.map(_.get)

  def mockIntStepFunction(int: Int, boolean: Boolean): PipelineStepResponse = {
    PipelineStepResponse(Some(int), Some(Map[String, Any]("boolean" -> boolean, "string" -> int)))
  }

  def mockIntListStepFunction(listSize: Int): PipelineStepResponse = {
    PipelineStepResponse(Some(List.tabulate(listSize)(_ + 1)), None)
  }

  def mockSumListOfInts(ints: List[Option[Int]]): Int = ints.map(_.get).sum

  def mockSumSimpleListOfInts(ints: List[Int]): Int ={
    ints.sum
  }

  def mockListOfIntsToString(ints: List[Int]): String ={
    ints.mkString(",")
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

  override def pipeline: Option[Pipeline] = None

  override def pipelineContext: PipelineContext = {
    PipelineContext(None, List[PipelineParameter](),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      } else {
        List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")
      }),
      PipelineStepMapper(),
      None, List(), PipelineManager(List()),
      None, new ContextManager(Map(), Map()), Map(), None)
  }
}
