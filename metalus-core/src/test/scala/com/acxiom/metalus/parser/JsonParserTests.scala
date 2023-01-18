package com.acxiom.metalus.parser

import com.acxiom.metalus.{MockClass, Pipeline, PipelineDefs}
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.funspec.AnyFunSpec

class JsonParserTests  extends AnyFunSpec {
  describe("DriverUtils - parsePipelineJson") {

    it("Should parse a basic pipeline json returning a list of Pipeline objects") {
      val json = JsonParser.serializePipelines(PipelineDefs.TWO_PIPELINE)
      val pipelineList = JsonParser.parsePipelineJson(json)
      assert(pipelineList.isDefined)
      assert(pipelineList.get.lengthCompare(2) == 0)
      verifyParsedPipelines(pipelineList.get.head, PipelineDefs.TWO_PIPELINE.head)
      verifyParsedPipelines(pipelineList.get(1), PipelineDefs.TWO_PIPELINE(1))
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
      val mc = JsonParser.parseJson(json, "com.acxiom.metalus.MockClass")
      assert(Option(mc).isDefined)
      assert(mc.asInstanceOf[MockClass].string == "Frederico")
    }
  }
}
