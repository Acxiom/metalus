package com.acxiom.pipeline.utils

import java.text.ParseException

import com.acxiom.pipeline.{MockClass, Pipeline, PipelineDefs}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.FunSpec

class DriverUtilsTests extends FunSpec {
  describe("DriverUtils - extractParameters") {
    it("Should properly parse input parameters") {
      val params = Array("--one", "1", "--two", "2", "--three", "true")
      val map = DriverUtils.extractParameters(params)
      assert(map("one") == "1")
      assert(map("two") == "2")
      assert(map("three").asInstanceOf[Boolean])
    }

    it("Should properly parse input parameters and fail on missing parameter") {
      val params = Array("--one", "1", "--two", "2", "--three", "true")
      val thrown = intercept[RuntimeException] {
        DriverUtils.extractParameters(params, Some(List("three", "four", "five")))
      }
      assert(thrown.getMessage.contains("Missing required parameters: four,five"))
    }
  }

  describe("DriverUtils - parsePipelineJson") {
    implicit val formats: Formats = DefaultFormats

    it("Should throw an exception when the json is not valid") {
      val json = s"""{"id": "1"}"""
      val thrown = intercept[ParseException] {
        DriverUtils.parsePipelineJson(json)
      }
      assert(thrown.getMessage.contains(json))
      assert(thrown.getErrorOffset == 0)
    }

    it("Should parse a basic pipeline json returning a list of Pipeline objects") {
      val json = Serialization.write(PipelineDefs.TWO_PIPELINE)
      val pipelineList = DriverUtils.parsePipelineJson(json)
      assert(pipelineList.isDefined)
      assert(pipelineList.get.lengthCompare(2) == 0)
      verifyParsedPipelines(pipelineList.get.head, PipelineDefs.TWO_PIPELINE.head)
      verifyParsedPipelines(pipelineList.get(1), PipelineDefs.TWO_PIPELINE(1))
    }

    it("Should throw and error when json is invalid") {
      val thrown = intercept[RuntimeException] {
        DriverUtils.parsePipelineJson("")
      }
      assert(Option(thrown).isDefined)
    }

    def verifyParsedPipelines(pipeline1: Pipeline, pipeline2: Pipeline): Unit = {
      assert(pipeline1.name.getOrElse("") == pipeline2.name.getOrElse("NONE"))
      assert(pipeline1.steps.isDefined)
      val steps = pipeline1.steps.get
      val steps1 = pipeline2.steps.get
      assert(steps.lengthCompare(steps.length) == 0)
      steps.foreach(step => {
        val step1 = steps1.find(s => s.id.getOrElse("") == step.id.getOrElse("NONE"))
        assert(step1.isDefined)
        assert(step.displayName.getOrElse("") == step1.get.displayName.getOrElse("NONE"))
        assert(step.`type`.getOrElse("") == step1.get.`type`.getOrElse("NONE"))
        assert(step.params.isDefined)
        assert(step.params.get.lengthCompare(step1.get.params.get.length) == 0)
      })
    }
  }

  describe("DriverUtils - JSON conversion") {
    it("Should convert JSON to case class") {
      val json =
        """
          |{
          | "string": "Frederico"
          |}
        """.stripMargin
      val mc = DriverUtils.parseJson(json, "com.acxiom.pipeline.MockClass")
      assert(Option(mc).isDefined)
      assert(mc.asInstanceOf[MockClass].string == "Frederico")
    }
  }
}

