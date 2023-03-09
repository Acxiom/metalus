package com.acxiom.metalus.spark.steps

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.metalus.spark._

import scala.annotation.tailrec

@StepObject
object SparkConfigurationSteps {

  @StepFunction("5c4d2d01-da85-4e2e-a551-f5a65f83653a",
    "Set Spark Local Property",
    "Set a property on the spark context.",
    "Pipeline", "Spark")
  @StepParameters(Map("key" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the property to set")),
    "value" -> StepParameter(None, Some(true), None, None, None, None, Some("The value to set"))))
  def setLocalProperty(key: String, value: Any, pipelineContext: PipelineContext): Unit = {
    setLocalProperties(Map(key -> value), None, pipelineContext)
  }

  @StepFunction("0b86b314-2657-4392-927c-e555af56b415",
    "Set Spark Local Properties",
    "Set each property on the spark context.",
    "Pipeline", "Spark")
  @StepParameters(Map("properties" -> StepParameter(None, Some(true), None, None, None, None,
    Some("Map representing local properties to set")),
    "keySeparator" -> StepParameter(None, Some(false), Some("__"), None, None, None,
      Some("String that will be replaced with a period character"))))
  def setLocalProperties(properties: Map[String, Any], keySeparator: Option[String] = None, pipelineContext: PipelineContext): Unit = {
    val sc = pipelineContext.sparkSession.sparkContext
    cleanseMap(properties, keySeparator).foreach {
      case (key, Some(value)) => sc.setLocalProperty(key, value.toString)
      case (key, None) => sc.setLocalProperty(key, None.orNull)
      case (key, value) => sc.setLocalProperty(key, value.toString)
    }
  }

  @StepFunction("c8c82365-e078-4a2a-99b8-0c0e20d8102d",
    "Set Hadoop Configuration Properties",
    "Set each property on the hadoop configuration.",
    "Pipeline", "Spark")
  @StepParameters(Map("properties" -> StepParameter(None, Some(true), None, None, None, None,
    Some("Map representing local properties to set")),
    "keySeparator" -> StepParameter(None, Some(false), Some("__"), None, None, None,
      Some("String that will be replaced with a period character"))))
  def setHadoopConfigurationProperties(properties: Map[String, Any], keySeparator: Option[String] = None,
                                       pipelineContext: PipelineContext): Unit = {
    val hc = pipelineContext.sparkSession.sparkContext.hadoopConfiguration
    cleanseMap(properties, keySeparator).foreach {
      case (key, Some(value)) => hc.set(key, value.toString)
      case (key, None) => hc.unset(key)
      case (key, value) => hc.set(key, value.toString)
    }
  }

  @StepFunction("ea7ea3e0-d1c2-40a2-b2b7-3488489509ca",
    "Set Hadoop Configuration Property",
    "Set a property on the hadoop configuration.",
    "Pipeline", "Spark")
  @StepParameters(Map("key" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the property to set")),
    "value" -> StepParameter(None, Some(true), None, None, None, None, Some("The value to set"))))
  def setHadoopConfigurationProperty(key: String, value: Any, pipelineContext: PipelineContext): Unit =
    setHadoopConfigurationProperties(Map(key -> value), None, pipelineContext)

  @StepFunction("b7373f02-4d1e-44cf-a9c9-315a5c1ccecc",
    "Set Job Group",
    "Set the current thread's group id and description that will be associated with any jobs.",
    "Pipeline", "Spark")
  @StepParameters(Map("groupId" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the group")),
    "description" -> StepParameter(None, Some(true), None, None, None, None, Some("Description of the job group")),
    "interruptOnCancel" -> StepParameter(None, Some(false), Some("false"), None, None, None,
      Some("When true, will trigger Thread.interrupt getting called on executor threads"))))
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Option[Boolean] = None,
                  pipelineContext: PipelineContext): Unit = {
    pipelineContext.sparkSession.sparkContext.setJobGroup(groupId, description, interruptOnCancel.getOrElse(false))
  }

  @StepFunction("7394ff4d-f74d-4c9f-a55c-e0fd398fa264",
    "Clear Job Group",
    "Clear the current thread's job group",
    "Pipeline", "Spark")
  def clearJobGroup(pipelineContext: PipelineContext): Unit = pipelineContext.sparkSession.sparkContext.clearJobGroup()


  @tailrec
  private def unwrapOptions(value: Any): Any = {
    value match {
      case Some(v: Option[_]) => unwrapOptions(v)
      case v => v
    }
  }

  private def cleanseMap(map: Map[String, Any], keySeparator: Option[String] = None): Map[String, Any] = {
    val sep = keySeparator.getOrElse("__")
    map.map { case (key, value) =>
      key.replaceAllLiterally(sep, ".") -> unwrapOptions(value)
    }
  }
}
