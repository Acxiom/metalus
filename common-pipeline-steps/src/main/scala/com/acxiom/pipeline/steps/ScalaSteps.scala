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
  def processScript(@StepParameter(Some("script"), Some(true), None, Some("scala")) script: String,
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
  def processScriptWithValue(@StepParameter(Some("script"), Some(true), None, Some("scala")) script: String,
                             value: Any, `type`: String = "Any",
                             pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScalaScriptEngine
    val bindings = engine.createBindings("logger", logger, "org.apache.log4j.Logger")
      .setBinding("userValue", value, `type`)
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
