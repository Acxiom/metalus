package com.acxiom.metalus.utils

import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.{PipelineContext, PipelineStepMapper}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class JavaScriptEngineTests extends AnyFunSpec with BeforeAndAfterAll {

  var scriptEngine: ScriptEngine = _

  override def beforeAll(): Unit = {
    scriptEngine = new JavaScriptEngine
  }

  describe("ScriptEngine - Simple Scripts") {
    it("Should run a simple Javascript") {
      val result = scriptEngine.executeSimpleScript("var a = 'a'; a + '_value'")
      assert(result == "a_value")
    }

    it("Should run a more complicated Javascript") {
      val js =
        """
          |var a = 'a';
          |var value = '_value';
          |var array = ['1', '2', '3'];
          |a + value + '-' + array.join('_');
        """.stripMargin
      val result = scriptEngine.executeSimpleScript(js)
      assert(result == "a_value-1_2_3")
    }

    it("Should execute a script against pipelineContext") {
      val pipelineContext = PipelineContext(Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        List(), Some(List("com.acxiom.metalus.steps", "com.acxiom.metalus")), PipelineStepMapper(), contextManager = new ContextManager(Map(), Map()))
      val js =
        """
          | pipelineContext.getGlobalString('pipelineId').get();
        """.stripMargin
      val result = scriptEngine.executeScript(js, pipelineContext)
      assert(result == "testPipelineId")

      val result1 = scriptEngine.executeScript(js, pipelineContext.setGlobal("pipelineId", "nextPipelineId"))
      assert(result1 == "nextPipelineId")
    }

    it("Should execute a script with a supplied object") {
      val pipelineContext = PipelineContext(Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        List(), Some(List("com.acxiom.metalus.steps", "com.acxiom.metalus")), PipelineStepMapper(),
        contextManager = new ContextManager(Map(), Map()))
      val js =
        """
          | 'RedOnTheHead' + userValue;
        """.stripMargin
      val result = scriptEngine.executeScriptWithObject(js, "Fred", pipelineContext)
      assert(result == "RedOnTheHeadFred")
    }
  }

  describe("ScriptEngine - Complex Scripts") {
    it("Should read from pipelineContext") {
      val pipelineContext = PipelineContext(Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        List(), Some(List("com.acxiom.metalus.steps", "com.acxiom.metalus")), PipelineStepMapper(),
        contextManager = new ContextManager(Map(), Map()))
      val js =
        """
          | var stringOption = pipelineContext.getGlobalString('pipelineId');
          | if (stringOption.isDefined()) {
          |   stringOption.get();
          | } else {
          |   '';
          | }
        """.stripMargin
      val result = scriptEngine.executeScript(js, pipelineContext)
      assert(result.asInstanceOf[String] == "testPipelineId")
    }

    it("Should modify pipelineContext") {
      val pipelineContext = PipelineContext(Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        List(), Some(List("com.acxiom.metalus.steps", "com.acxiom.metalus")), PipelineStepMapper(),
        contextManager = new ContextManager(Map(), Map()))
      val js =
        """
          | pipelineContext.setGlobal('jsString', 'Should create a new global');
        """.stripMargin
      val result = scriptEngine.executeScript(js, pipelineContext)
      val ctx = result.asInstanceOf[PipelineContext]
      assert(ctx.getGlobalString("jsString").getOrElse("none") == "Should create a new global")
    }
  }
}
