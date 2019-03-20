package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter}
import com.acxiom.pipeline.utils.JavaScriptEngine
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}
import javax.script.SimpleBindings
import org.apache.log4j.Logger

@StepObject
object JavascriptSteps {
  private val logger = Logger.getLogger(getClass)
  @StepFunction("5e0358a0-d567-5508-af61-c35a69286e4e",
    "Javascript Step",
    "Executes a script and returns the result",
    "Pipeline",
    "Scripting")
  def processScript(@StepParameter(Some("script"), Some(true), None, Some("javascript")) script: String,
                    pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new JavaScriptEngine
    val bindings = new SimpleBindings()
    bindings.put("logger", logger)
    val result = engine.executeScript(script, bindings, pipelineContext)
    handleResult(result)
  }

  @StepFunction("570c9a80-8bd1-5f0c-9ae0-605921fe51e2",
    "Javascript Step with additional object provided",
    "Executes a script and returns the result",
    "Pipeline",
    "Scripting")
  def processScriptWithValue(@StepParameter(Some("script"), Some(true), None, Some("javascript")) script: String,
                             value: Any, pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new JavaScriptEngine
    val bindings = new SimpleBindings()
    bindings.put("logger", logger)
    val result = engine.executeScriptWithObject(script, value, bindings, pipelineContext)
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
