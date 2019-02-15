package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext

trait ScriptEngine {

  val engineName: String

  /**
    * This function will execute a simple self-contained script and return the result.
    *
    * @param script The script to execute.
    * @return The result of the execution.
    */
  def executeSimpleScript(script: String): Any

  /**
    * This function will execute a script with access to the "pipelineContext" object.
    *
    * @param script          The script to execute.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScript(script: String, pipelineContext: PipelineContext): Any

  /**
    * This function will execute a script with access to the "pipelineContext" object and the provided "obj".
    *
    * @param script          The script to execute.
    * @param userValue       The object to make accessible to the script.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScriptWithObject(script: String, userValue: Any, pipelineContext: PipelineContext): Any

}
