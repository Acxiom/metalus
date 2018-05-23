package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext
import javax.script.{Compilable, ScriptEngineManager, SimpleBindings}

class ScriptEngine {
  private val engine = new ScriptEngineManager().getEngineByName("Nashorn").asInstanceOf[Compilable]

  /**
    * This function will execute a simple self-contained javascript and return the result.
    *
    * @param script The script to execute.
    * @return The result of the execution.
    */
  def executeSimpleScript(script: String): Any = {
    engine.compile(script).eval()
  }

  /**
    * This function will execute a javascript with access to the "pipelineContext" object.
    *
    * @param script          The script to execute.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScript(script: String, pipelineContext: PipelineContext): Any = {
    // Add it to the script context
    val bindings = new SimpleBindings()
    bindings.put("pipelineContext", pipelineContext)
    engine.compile(script).eval(bindings)
  }

  /**
    * This function will execute a javascript with access to the "pipelineContext" object and the provided "obj".
    *
    * @param script          The script to execute.
    * @param obj             The object to make accessible to the script.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScriptWithObject(script: String, obj: Any, pipelineContext: PipelineContext): Any = {
    // Add it to the script context
    val bindings = new SimpleBindings()
    bindings.put("pipelineContext", pipelineContext)
    bindings.put("obj", obj)
    engine.compile(script).eval(bindings)
  }
}
