package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

class ScalaScriptEngine extends ScriptEngine {

  override val engineName: String = "scala"

  /**
    * This function will execute a simple self-contained scala script and return the result.
    *
    * @param script The script to execute.
    * @return The result of the execution.
    */
  override def executeSimpleScript(script: String): Any = {
    val r = compile[Any](script)
    r()
  }

  /**
    * This function will execute a scala script with access to the "pipelineContext" object.
    *
    * @param script          The script to execute.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  override def executeScript(script: String, pipelineContext: PipelineContext): Any = {
    executeScriptWithObject(script, None, pipelineContext)
  }

  /**
    * This function will execute a scala script with access to the "pipelineContext" object and the provided "obj".
    *
    * @param script          The script to execute.
    * @param userValue       The object to make accessible to the script.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  override def executeScriptWithObject(script: String, userValue: Any, pipelineContext: PipelineContext): Any = {
    val r = compile[Any, Any](script)
    r(userValue, pipelineContext)
  }

  private def compile[A, B](code: String)( implicit m: ClassTag[A]): (A, PipelineContext) => B = {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val tree = tb.parse(
      s"""
         |def wrapper(userValue: ${m.runtimeClass.getCanonicalName}, pipelineContext: com.acxiom.pipeline.PipelineContext): Any = {
         |  $code
         |}
         |wrapper _
      """.stripMargin)
    tb.compile(tree)().asInstanceOf[(A, PipelineContext) => B]
  }

  private def compile[A](code: String): () => A = {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val tree = tb.parse(code.stripMargin)
    tb.compile(tree)().asInstanceOf[() => A]
  }
}
