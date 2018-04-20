package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext
import javax.script.ScriptEngineManager
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.Serialization

class ScriptEngine {
  private implicit val formats: Formats = DefaultFormats
  private val engine = new ScriptEngineManager().getEngineByMimeType("text/javascript")

  /**
    * This function will execute a simple self-contained javascript and return the result.
    * @param script The script to execute.
    * @return The result of the execution.
    */
  def executeSimpleScript(script: String): Any = {
    engine.eval(script)
  }

  /**
    * This function will execute a javascript with access to the "globals" object.
    * @param script The script to execute.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScript(script: String, pipelineContext: PipelineContext): Any = {
    // Serialize the globals object
    val globals = Serialization.write(pipelineContext.globals)
    // Add it to the script context
    engine.put("GLOBALS", globals)
    engine.eval(
      s"""
         |// Parse the GLOBALS back into a JSON objetc
         |var globals = JSON.parse(GLOBALS);
         |
         |$script""".stripMargin)
  }
}
