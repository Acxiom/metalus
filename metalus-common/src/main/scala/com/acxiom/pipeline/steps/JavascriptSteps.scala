package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.utils.JavaScriptEngine
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}
import org.apache.log4j.Logger

import javax.script.SimpleBindings

@StepObject
object JavascriptSteps {
  private val logger = Logger.getLogger(getClass)
  @StepFunction("5e0358a0-d567-5508-af61-c35a69286e4e",
    "Javascript Step",
    "Executes a script and returns the result",
    "Pipeline",
    "Scripting")
  @StepParameters(Map("script" -> StepParameter(Some("script"), Some(true), None,
    Some("javascript"), None, None, Some("Javascript to execute"))))
  def processScript(script: String,
                    pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new JavaScriptEngine
    val bindings = new SimpleBindings()
    bindings.put("logger", logger)
    val result = engine.executeScript(script, bindings, pipelineContext)
    handleResult(result)
  }

  @StepFunction("570c9a80-8bd1-5f0c-9ae0-605921fe51e2",
    "Javascript Step with single object provided",
    "Executes a script with single object provided and returns the result",
    "Pipeline",
    "Scripting")
  @StepParameters(Map("script" -> StepParameter(Some("script"), Some(true), None, Some("javascript"), None, None, Some("Javascript script to execute")),
    "value" -> StepParameter(None, Some(true), None, None, None, None, Some("Value to bind to the script"))))
  def processScriptWithValue(script: String,
                             value: Any, pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new JavaScriptEngine
    val bindings = new SimpleBindings()
    bindings.put("logger", logger)
    val result = engine.executeScriptWithObject(script, value, bindings, pipelineContext)
    handleResult(result)
  }

  @StepFunction("f92d4816-3c62-4c29-b420-f00994bfcd86",
    "Javascript Step with map of objects provided",
    "Executes a script with map of objects provided and returns the result",
    "Pipeline",
    "Scripting")
  @StepParameters(Map("script" -> StepParameter(Some("script"), Some(true), None, Some("javascript"), None, None, Some("Javascript script to execute")),
    "values" -> StepParameter(None, Some(true), None, None, None, None, Some("Map of name/value pairs to bind to the script")),
    "unwrapOptions" -> StepParameter(None, Some(false), None, None, None, None, Some("Flag to control option unwrapping behavior"))))
  def processScriptWithValues(@StepParameter(Some("script"), Some(true), None, Some("javascript"), None, None) script: String,
                             values: Map[String, Any], unwrapOptions: Option[Boolean] = None, pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new JavaScriptEngine
    val bindings = new SimpleBindings()
    bindings.put("logger", logger)
    values.foreach{
      case (name, value) =>
        val unwrapped = value match {
          case s: Some[_] if unwrapOptions.getOrElse(true) => s.get
          case None if unwrapOptions.getOrElse(true) => None.orNull
          case v => v
        }
        bindings.put(name, unwrapped)
    }
    val result = engine.executeScript(script, bindings, pipelineContext)
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
