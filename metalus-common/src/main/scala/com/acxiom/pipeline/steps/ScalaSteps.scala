package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter}
import com.acxiom.pipeline.utils.ScalaScriptEngine
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}
import org.apache.log4j.Logger

@StepObject
object ScalaSteps {
  private val logger = Logger.getLogger(getClass)
  @StepFunction("a7e17c9d-6956-4be0-a602-5b5db4d1c08b",
    "Scala script Step",
    "Executes a script and returns the result",
    "Pipeline",
    "Scripting")
  def processScript(@StepParameter(Some("script"), Some(true), None, Some("scala"), None, None) script: String,
                    pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScalaScriptEngine
    val result = engine.executeScript(script, pipelineContext)
    handleResult(result)
  }

  @StepFunction("8bf8cef6-cf32-4d85-99f4-e4687a142f84",
    "Scala script Step with additional object provided",
    "Executes a script with the provided object and returns the result",
    "Pipeline",
    "Scripting")
  def processScriptWithValue(@StepParameter(Some("script"), Some(true), None, Some("scala"), None, None) script: String,
                             value: Any, `type`: Option[String] = None,
                             pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScalaScriptEngine
    val bindings = engine.createBindings("logger", logger, Some("org.apache.log4j.Logger"))
      .setBinding("userValue", value, `type`)
    val result = engine.executeScriptWithBindings(script, bindings, pipelineContext)
    handleResult(result)
  }

  @StepFunction("3ab721e8-0075-4418-aef1-26abdf3041be",
    "Scala script Step with additional objects provided",
    "Executes a script with the provided object and returns the result",
    "Pipeline",
    "Scripting")
  def processScriptWithValues(@StepParameter(Some("script"), Some(true), None, Some("scala"), None, None) script: String,
                              values: Map[String, Any],
                              types: Option[Map[String, String]] = None,
                              unwrapOptions: Option[Boolean] = None,
                              pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScalaScriptEngine
    val typeMappings = types.getOrElse(Map())
    val initialBinding = engine.createBindings("logger", logger, Some("org.apache.log4j.Logger"))
    val bindings = values.foldLeft(initialBinding) { (bindings, pair) =>
      val value = pair._2 match {
        case s: Some[_] if unwrapOptions.getOrElse(true) => s.get
        case v => v
      }
      bindings.setBinding(pair._1, value, typeMappings.get(pair._1))
    }
    val result = engine.executeScriptWithBindings(script, bindings, pipelineContext)
    handleResult(result)
  }

  /**
    * This function will take the provided result value and wrap it in a PipelineStepResponse. If the result is already
    * wrapped in an Option, it will be used as is otherwise it will be wrapped in an Option.
    *
    * @param result The result value to wrap.
    * @return A PipelineStepResponse containing the result as the primary value.
    */
  private def handleResult(result: Any): PipelineStepResponse = {
    result match {
      case response: PipelineStepResponse => response
      case r: Option[_] => PipelineStepResponse(r.asInstanceOf[Option[Any]], None)
      case _ => PipelineStepResponse(Some(result), None)
    }
  }
}
