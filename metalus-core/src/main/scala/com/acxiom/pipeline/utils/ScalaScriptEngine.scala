package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

class ScalaScriptEngine extends ScriptEngine {

  override val engineName: String = "scala"
  private val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

  /**
    * This function will execute a simple self-contained scala script and return the result.
    *
    * @param script The script to execute.
    * @return The result of the execution.
    */
  override def executeSimpleScript(script: String): Any = {
    compile[Any](script)
  }

  /**
    * This function will execute a scala script with access to the "pipelineContext" object.
    *
    * @param script          The script to execute.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  override def executeScript(script: String, pipelineContext: PipelineContext): Any = {
    val r = compileWithBindings(script, None)
    r(None, pipelineContext)
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
    val bindings = Some(createBindings("userValue", userValue))
    val r = compileWithBindings(script, bindings)
    r(bindings, pipelineContext)
  }

  /**
    * This function will execute a scala script with access to the "pipelineContext" object and the provided Bindings.
    *
    * @param script          The script to execute.
    * @param bindings        The Bindings object to make accessible to the script.
    * @param pipelineContext The pipelineContext containing the globals.
    * @return The result of the execution.
    */
  def executeScriptWithBindings(script: String, bindings: Bindings, pipelineContext: PipelineContext): Any = {
    val r = compileWithBindings(script, Some(bindings))
    r(Some(bindings), pipelineContext)
  }

  /**
    *
    * @param name   The binding name.
    * @param value  The value to bind.
    * @param `type` The type of the value.
    * @return A Bindings object.
    */
  def createBindings(name: String, value: Any, `type`: String = "Any"): Bindings = {
    Bindings(Map[String, Binding](name -> Binding(name, value, `type`)))
  }

  private def compileWithBindings(code: String, bindings: Option[Bindings]): (Option[Bindings], PipelineContext) => Any = {
    val vals = if (bindings.isDefined) {
      bindings.get.bindings.map(b => getValString(b._2)).mkString("\n")
    } else {
      ""
    }
    val script =
      s"""
         |def wrapper(bindings: Option[com.acxiom.pipeline.utils.Bindings], pipelineContext: com.acxiom.pipeline.PipelineContext): Any = {
         |  $vals
         |  $code
         |}
         |wrapper _
      """.stripMargin
    val tree = toolBox.parse(script.stripMargin)
    toolBox.compile(tree)().asInstanceOf[(Option[Bindings], PipelineContext) => Any]
  }

  private def compile[A](code: String): A = {
    val tree = toolBox.parse(code.stripMargin)
    toolBox.compile(tree)().asInstanceOf[A]
  }

  private def getValString(binding: Binding): String = {
    if (binding.`type` == "Any") {
      s"""val ${binding.name} = bindings.get.getBinding("${binding.name}").value"""
    } else {
      s"""val ${binding.name} = bindings.get.getBinding("${binding.name}").value.asInstanceOf[${binding.`type`}]"""
    }
  }
}

case class Bindings(bindings: Map[String, Binding] = Map[String, Binding]()) {

  def setBinding(name: String, value: Any, `type`: String = "Any"): Bindings = {
    this.copy(bindings = this.bindings ++ Map[String, Binding](name -> Binding(name, value, `type`)))
  }

  def getBinding(name: String): Binding = {
    bindings(name)
  }
}

case class Binding(name: String, value: Any, `type`: String = "Any")
