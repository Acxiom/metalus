package com.acxiom.pipeline.utils

import com.acxiom.pipeline.{PipelineStepResponse, _}
import org.scalatest.FunSpec

class ReflectionUtilsTests extends FunSpec {
  private val FIVE = 5
  describe("ReflectionUtil - processStep") {
    val pipelineContext = PipelineContext(None, None, None, PipelineSecurityManager(), PipelineParameters(),
      Some(List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")), PipelineStepMapper(), None, None)
    it("Should process step function") {
      val step = PipelineStep(None, None, None, None, None, Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("string" -> "string", "boolean" -> true), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "string")
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.get("boolean").asInstanceOf[Boolean])
    }

    it("Should process step function with non-PipelineStepResponse") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse"))))
      val response = ReflectionUtils.processStep(step, Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "string")
    }

    it("Should process step with Option") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("string" -> "string", "boolean" -> Some(true), "opt" -> "Option"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "string")
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.get("boolean").asInstanceOf[Boolean])
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.get("option").asInstanceOf[Option[String]].getOrElse("") == "Option")
    }
  }

  describe("ReflectionUtils - loadClass") {
    it("Should instantiate a class") {
      val className = "com.acxiom.pipeline.MockClass"
      val mc = ReflectionUtils.loadClass(className, Some(Map[String, Any]("string" -> "my_string")))
      assert(mc.isInstanceOf[MockClass])
      assert(mc.asInstanceOf[MockClass].string == "my_string")
    }

    it("Should instantiate a complex class") {
      val className = "com.acxiom.pipeline.MockDriverSetup"
      val params = Map[String, Any]("parameters" -> Map[String, Any]("initialPipelineId" -> "pipelineId1"))
      val mockDriverSetup = ReflectionUtils.loadClass(className, Some(params))
      assert(mockDriverSetup.isInstanceOf[MockDriverSetup])
      assert(mockDriverSetup.asInstanceOf[MockDriverSetup].initialPipelineId == "pipelineId1")
    }

    it("Should instantiate no param constructor") {
      val className = "com.acxiom.pipeline.MockNoParams"
      val mc = ReflectionUtils.loadClass(className, None)
      assert(mc.isInstanceOf[MockNoParams])
      assert(mc.asInstanceOf[MockNoParams].string == "no-constructor-string")
    }
  }

  describe("ReflectionUtils - extractField") {
    it("Should return None when field name is invalid") {
      assert(ReflectionUtils.extractField(MockObject("string", FIVE), "bob").asInstanceOf[Option[_]].isEmpty)
    }

    it("Should return None when entity is None") {
      assert(ReflectionUtils.extractField(None, "something").asInstanceOf[Option[_]].isEmpty)
    }
  }
}

case class MockObject(string: String, num: Int, opt: Option[String] = None)
