package com.acxiom.pipeline.utils

import com.acxiom.pipeline.{PipelineContext, PipelineParameters, PipelineSecurityManager, PipelineStepMapper}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class ScalaScriptEngineTests extends FunSpec with BeforeAndAfterAll {

  var scriptEngine: ScriptEngine = _

  override def beforeAll(): Unit = {
    scriptEngine = new ScalaScriptEngine
  }

  describe("ScriptEngine - Simple Scripts") {

    it("Should run a simple Javascript") {
      val result = scriptEngine.executeSimpleScript(
        """
          | val a = "a"
          | a + "_value"
          | """.stripMargin)
      assert(result == "a_value")
    }

    it("Should run a more complicated Javascript") {
      val script =
        """
          |val a = "a"
          |var value = "_value"
          |val list = List[String]("1", "2", "3");
          |a + value + '-' + list.mkString("_");
        """.stripMargin
      val result = scriptEngine.executeSimpleScript(script)
      assert(result == "a_value-1_2_3")
    }

    it("Should execute a script against pipelineContext") {
      val pipelineContext = PipelineContext(None, None, Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        PipelineSecurityManager(), PipelineParameters(),
        Some(List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")), PipelineStepMapper(), None, None)
      val script =
        """
          | pipelineContext.getGlobalString("pipelineId").get
        """.stripMargin
      val result = scriptEngine.executeScript(script, pipelineContext)
      assert(result == "testPipelineId")

      val result1 = scriptEngine.executeScript(script, pipelineContext.setGlobal("pipelineId", "nextPipelineId"))
      assert(result1 == "nextPipelineId")
    }

    it("Should execute a script with a supplied object") {
      val pipelineContext = PipelineContext(None, None, Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        PipelineSecurityManager(), PipelineParameters(),
        Some(List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")), PipelineStepMapper(), None, None)
      val script =
        """
          | "RedOnTheHead" + userValue.asInstanceOf[String]
        """.stripMargin
      val result = scriptEngine.executeScriptWithObject(script, "Fred", pipelineContext)
      assert(result == "RedOnTheHeadFred")
    }
  }

  describe("ScriptEngine - Complex Scripts") {
    it("Should read from pipelineContext") {
      val pipelineContext = PipelineContext(None, None, Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        PipelineSecurityManager(), PipelineParameters(),
        Some(List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")), PipelineStepMapper(), None, None)
      val script =
        """
          | var stringOption = pipelineContext.getGlobalString("pipelineId")
          | if (stringOption.isDefined) {
          |   stringOption.get
          | } else {
          |   ""
          | }
        """.stripMargin
      val result = scriptEngine.executeScript(script, pipelineContext)
      assert(result.asInstanceOf[String] == "testPipelineId")
    }

    it("Should modify pipelineContext") {
      val pipelineContext = PipelineContext(None, None, Some(Map[String, Any]("pipelineId" -> "testPipelineId")),
        PipelineSecurityManager(), PipelineParameters(),
        Some(List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")), PipelineStepMapper(), None, None)
      val script =
        """
          | pipelineContext.setGlobal("scalaString", "Should create a new global");
        """.stripMargin
      val result = scriptEngine.executeScript(script, pipelineContext)
      val ctx = result.asInstanceOf[PipelineContext]
      assert(ctx.getGlobalString("scalaString").getOrElse("none") == "Should create a new global")
    }
  }

}
