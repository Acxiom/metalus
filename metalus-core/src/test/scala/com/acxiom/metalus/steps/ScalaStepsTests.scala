package com.acxiom.metalus.steps

import com.acxiom.metalus._
import com.acxiom.metalus.context.ContextManager
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.slf4j.event.Level
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{FileSystems, Files, StandardCopyOption}

class ScalaStepsTests extends AnyFunSpec with BeforeAndAfterAll with GivenWhenThen {
  var pipelineContext: PipelineContext = _

  override def beforeAll(): Unit = {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)

    pipelineContext = PipelineContext(Some(Map[String, Any]()),
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
  }

  override def afterAll(): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).atLevel(Level.INFO)
  }

  describe("ScalaSteps - Basic scripting") {
    // Copy file
    val tempFile = File.createTempFile("testFile", ".csv")
    tempFile.deleteOnExit()
    Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
      FileSystems.getDefault.getPath(tempFile.getAbsolutePath),
      StandardCopyOption.REPLACE_EXISTING)

    val script =
      """
        |import java.io._
        |import scala.io.Source
        |
        |Source.fromInputStream(new FileInputStream($path)).getLines().toList
        |""".stripMargin

    it("Should load a file using Scala") {
      val updatedScript = script.replaceAll("\\$path", "\"" + tempFile.getAbsolutePath + "\"")
      val result = ScalaSteps.processScript(updatedScript, pipelineContext)
      assert(result.primaryReturn.isDefined)
      val df = result.primaryReturn.get.asInstanceOf[List[String]]
      assert(df.length == 1001)
    }

    it ("Should load a file using Scala and a provide user value") {
      val updatedScript = script.replaceAll("\\$path", "userValue.asInstanceOf[String]")
      val result = ScalaSteps.processScriptWithValue(script = updatedScript,
        value = tempFile.getAbsolutePath,
        pipelineContext= pipelineContext)
      assert(result.primaryReturn.isDefined)
      val df = result.primaryReturn.get.asInstanceOf[List[String]]
      assert(df.length == 1001)
    }

    it("Should handle typed bindings"){
      val updatedScript = script.replaceAll("\\$path", "userValue")
      val result = ScalaSteps.processScriptWithValue(updatedScript,
        tempFile.getAbsolutePath,
        Some("String"),
        pipelineContext)
      assert(result.primaryReturn.isDefined)
      val df = result.primaryReturn.get.asInstanceOf[List[String]]
      assert(df.length == 1001)
    }

    it("Should handle multiple values and derive types"){
      val scriptWithDerivedTypes =
        """
          |val tmp = if (v2) v1 + v4.last.asInstanceOf[Int] else -1
          |(v3.toUpperCase, tmp, v5.toString)
          |""".stripMargin
      val mappings: Map[String, Any] = Map(
        "v1" -> 1,
        "v2" -> true,
        "v3" -> "chicken",
        "v4" -> List(1,2,3),
        "v5" -> ChickenColors.BUFF
      )
      val result = ScalaSteps.processScriptWithValues(scriptWithDerivedTypes, mappings, None, None, pipelineContext)
      assert(result.primaryReturn.isDefined)
      val tuple = result.primaryReturn.get.asInstanceOf[(String, Int, String)]
      assert(tuple._1 == "CHICKEN")
      assert(tuple._2 == 4)
      assert(tuple._3 == "BUFF")
    }

    it("Should respect the unwrapOptions flag"){
      val scriptWithDerivedTypes =
        """
          |value.toString
          |""".stripMargin
      val mappings: Map[String, Any] = Map(
        "value" -> Some("chicken")
      )
      val result = ScalaSteps.processScriptWithValues(scriptWithDerivedTypes, mappings, None, Some(false), pipelineContext)
      assert(result.primaryReturn.isDefined)
      val optionString = result.primaryReturn.get.asInstanceOf[String]
      assert(optionString == "Some(chicken)")
      val unwrapResult = ScalaSteps.processScriptWithValues(scriptWithDerivedTypes, mappings, None, None, pipelineContext)
      assert(unwrapResult.primaryReturn.isDefined)
      val unrwappedString = unwrapResult.primaryReturn.get.asInstanceOf[String]
      assert(unrwappedString == "chicken")
    }
  }
}

object ChickenColors extends Enumeration {
  type ChickenColors = Value
  val GOLD, WHITE, BLACK, BUFF, GRAY = Value
}
