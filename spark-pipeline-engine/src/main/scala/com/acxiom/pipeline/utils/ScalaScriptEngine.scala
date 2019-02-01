package com.acxiom.pipeline.utils

import com.acxiom.pipeline.PipelineContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

class ScalaScriptEngine extends ScriptEngine {

  override val engineName: String = "scala"
  private val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
  private var bindings: Option[Bindings] = None

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
    val r = compileWithBindings(script, bindings)
    r(bindings, pipelineContext)
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
    setBinding("userValue", userValue)
    val r = compileWithBindings(script, bindings)
    r(bindings, pipelineContext)
  }

  private def compileWithBindings(code: String, bindings: Option[Bindings]): (Option[Bindings], PipelineContext) => Any = {
    val vals = if (bindings.isDefined) {
      bindings.get.bindings.map(b => b._2.getVarString(b._1)).mkString("\n")
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

  private def compile[A](code: String):  A = {
    val tree = toolBox.parse(code.stripMargin)
    toolBox.compile(tree)().asInstanceOf[A]
  }

  def setBinding(name: String, value: Any, `type`: String = "Any"): Unit = {
    if (bindings.isDefined) {
      bindings = Some(bindings.get.setBinding(name, value, `type`))
    } else {
      bindings = Some(Bindings(bindings = Map(name -> TypedVal(value, `type`))))
    }
  }
}

case class Bindings(bindings: Map[String, TypedVal] = Map[String, TypedVal]()) {

  def setBinding(name: String, value: Any, `type`: String = "Any"): Bindings = {
    this.copy(bindings = this.bindings ++ Map[String, TypedVal](name -> TypedVal(value, `type`)))
  }

  def getBinding(name: String): TypedVal = {
    bindings(name)
  }
}

case class TypedVal(value: Any, `type`: String = "Any") {

  def getVarString(name: String): String = {
    if (`type` == "Any") {
      s"""val ${name} = bindings.get.getBinding("${name}").value"""
    } else {
      s"""val ${name} = bindings.get.getBinding("${name}").value.asInstanceOf[${`type`}]"""
    }
  }
}
