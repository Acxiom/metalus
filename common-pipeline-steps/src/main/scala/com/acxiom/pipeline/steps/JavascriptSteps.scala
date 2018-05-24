package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.utils.ScriptEngine
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}

@StepObject
object JavascriptSteps {
  @StepFunction("5e0358a0-d567-5508-af61-c35a69286e4e",
    "Javascript Step",
    "Executes a Javascript and returns the result",
    "Pipeline")
  def processScript(script: String, pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScriptEngine
    val result = engine.executeScript(script, pipelineContext)
    handleResult(result)
  }

  @StepFunction("570c9a80-8bd1-5f0c-9ae0-605921fe51e2",
    "Javascript Step with additional object provided",
    "Executes a Javascript and returns the result",
    "Pipeline")
  def processScriptWithValue(script: String, value: Any, pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScriptEngine
    val result = engine.executeScriptWithObject(script, value, pipelineContext)
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
