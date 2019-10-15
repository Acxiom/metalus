package com.acxiom.pipeline.utils

import java.util

import com.acxiom.pipeline.{PipelineStepResponse, _}
import org.scalatest.FunSpec

class ReflectionUtilsTests extends FunSpec {
  private val FIVE = 5
  describe("ReflectionUtil - processStep") {
    val pipeline = Pipeline(Some("TestPipeline"))
    val pipelineContext = PipelineContext(None, None, None, PipelineSecurityManager(), PipelineParameters(),
      Some(List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")), PipelineStepMapper(), None, None)
    it("Should process step function") {
      val step = PipelineStep(None, None, None, None, None, Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
      val response = ReflectionUtils.processStep(step, pipeline,
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
      val response = ReflectionUtils.processStep(step, pipeline, Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "string")
    }

    it("Should process step with Option") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
      val response = ReflectionUtils.processStep(step, pipeline,
        Map[String, Any]("string" -> "string", "boolean" -> Some(true), "opt" -> "Option"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "string")
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.get("boolean").asInstanceOf[Boolean])
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].namedReturns.get("option").asInstanceOf[Option[String]].getOrElse("") == "Option")
    }

    it("Should process step with default value") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithDefaultValue"))))
      val response = ReflectionUtils.processStep(step, pipeline,
        Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.get == "chicken")
    }

    it("Should wrap values in a List, Seq, or Array if passing a single element to a collection") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithListParams"))))
      val response = ReflectionUtils.processStep(step, pipeline,
        Map[String, Any]("list" -> "l1", "seq" -> 1, "arrayList" -> new util.ArrayList()), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.get == "Some(l1),Some(1),None")
    }

    it("Should return an informative error if a step function is not found") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.typo"))))
      val thrown = intercept[IllegalArgumentException]{
        val response = ReflectionUtils.processStep(step, pipeline, Map[String, Any](), pipelineContext)
      }
      assert(thrown.getMessage == "typo is not a valid function!")
    }

    it("Should return an informative error if the parameter types do not match function params"){
      val step = PipelineStep(Some("chicken"), None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val thrown = intercept[PipelineException] {
        ReflectionUtils.processStep(step, pipeline, Map[String, Any]("string" -> 1), pipelineContext)
      }
      val message = "Failed to map value [Some(1)] of type [Some(Integer)] to paramName [string] of" +
        " type [Option[String]] for method [mockStepFunctionWithOptionalGenericParams] in step [chicken] in pipeline [TestPipeline]"
      assert(thrown.getMessage == message)
    }

    it("should handle primitive types"){
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithPrimitives"))))
      val map = Map[String, Any]("i" -> 1,
        "l" -> 1L,
        "s" -> 1.toShort.asInstanceOf[java.lang.Short],
        "d" -> 1.0D,
        "f" -> 1.0F,
        "c" -> '1',
        "by" -> 1.toByte.asInstanceOf[java.lang.Byte],
        "a" -> "anyValue"
      )
      val response = ReflectionUtils.processStep(step, pipeline, map, pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.get == 1)
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
