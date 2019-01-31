package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext
import javax.script.{Bindings, Compilable, ScriptEngineManager, SimpleBindings}

class JavaScriptEngine extends ScriptEngine {

  val engineName: String = "Nashorn"
  private val engine = new ScriptEngineManager().getEngineByName(engineName).asInstanceOf[Compilable]

  /**
    * This function will execute a simple self-contained javascript and return the result.
    *
    * @param script The script to execute.
    * @return The result of the execution.
    */
  override def executeSimpleScript(script: String): Any = {
    engine.compile(script).eval()
  }

  /**
    * This function will execute a javascript with access to the "pipelineContext" object.
    *
    * @param script          The script to execute.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  override def executeScript(script: String, pipelineContext: PipelineContext): Any = {
    executeScript(script, new SimpleBindings(), pipelineContext)
  }

  /**
    * This function will execute a javascript with access to the "pipelineContext" object.
    *
    * @param script          The script to execute.
    * @param bindings        Bindings to use when executing the script
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScript(script: String, bindings: Bindings, pipelineContext: PipelineContext): Any = {
    // Add it to the script context
    setDefaultBindings(bindings, pipelineContext)
    engine.compile(script).eval(bindings)
  }

  /**
    * This function will execute a javascript with access to the "pipelineContext" object and the provided "obj".
    *
    * @param script          The script to execute.
    * @param userValue       The object to make accessible to the script.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  override def executeScriptWithObject(script: String, userValue: Any, pipelineContext: PipelineContext): Any = {
    executeScriptWithObject(script, userValue, new SimpleBindings(), pipelineContext)
  }

  /**
    * This function will execute a javascript with access to the "pipelineContext" object and the provided "obj".
    *
    * @param script          The script to execute.
    * @param userValue       The object to make accessible to the script.
    * @param bindings        Bindings to use when executing the script
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScriptWithObject(script: String, userValue: Any, bindings: Bindings, pipelineContext: PipelineContext): Any = {
    // Add it to the script context
    setDefaultBindings(bindings, pipelineContext)
    bindings.put("userValue", userValue)
    engine.compile(script).eval(bindings)
  }

  private def setDefaultBindings(bindings: Bindings, pipelineContext: PipelineContext): Unit = {
    bindings.put("pipelineContext", pipelineContext)
    bindings.put("ReflectionUtils", ReflectionUtils)
  }
}
