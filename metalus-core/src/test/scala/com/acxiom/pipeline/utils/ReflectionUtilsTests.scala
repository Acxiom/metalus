package com.acxiom.pipeline.utils

import java.util

import com.acxiom.pipeline.{PipelineStepResponse, _}
import org.scalatest.FunSpec

class ReflectionUtilsTests extends FunSpec {
  private val FIVE = 5
  describe("ReflectionUtil - processStep") {
    val pipeline = Pipeline(Some("TestPipeline"))
    val globals = Map[String, Any]("validateStepParameterTypes" -> true)
    val pipelineContext = PipelineContext(None, None, Some(globals), PipelineSecurityManager(), PipelineParameters(),
      Some(List("com.acxiom.pipeline", "com.acxiom.pipeline.steps")), PipelineStepMapper(), None, None)
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
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse"), Some("com.acxiom.pipeline.steps"))))
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

    it("Should unwrap an option passed to a non option param"){
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepWithListOfOptions"))))
      val response = ReflectionUtils.processStep(step, pipeline,
        Map[String, Any]("s" -> Some(List(Some("s1"), Some("s2")))), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.get == "s1,s2")
    }

    it("Should return an informative error if a step function is not found") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.typo"))))
      val thrown = intercept[IllegalArgumentException] {
        ReflectionUtils.processStep(step, pipeline, Map[String, Any](), pipelineContext)
      }
      assert(thrown.getMessage == "typo is not a valid function!")
    }

    it("Should respect the validateStepParameterTypes global") {
      val step = PipelineStep(Some("chicken"), None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val thrown = intercept[ClassCastException] {
        ReflectionUtils.processStep(step, pipeline, Map[String, Any]("string" -> 1), pipelineContext.setGlobal("validateStepParameterTypes", false))
      }
      val message = "java.lang.Integer cannot be cast to java.lang.String"
      assert(thrown.getMessage == message)
    }

    it("Should return an informative error if the parameter types do not match function params") {
      val step = PipelineStep(Some("chicken"), None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val thrown = intercept[PipelineException] {
        ReflectionUtils.processStep(step, pipeline, Map[String, Any]("string" -> 1), pipelineContext)
      }
      val message = "Failed to map value [Some(1)] of type [Some(Integer)] to paramName [string] of" +
        " type [Option[String]] for method [mockStepFunctionWithOptionalGenericParams] in step [chicken] in pipeline [TestPipeline]"
      assert(thrown.getMessage == message)
    }

    it("should handle primitive types") {
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

    it("Should respect the pkg setting on EngineMeta") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse"), Some("com.acxiom.pipeline.steps"))))
      val response = ReflectionUtils.processStep(step, pipeline, Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "string")
    }

    it("Should inject None for missing parameters") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val response = ReflectionUtils.processStep(step, pipeline, Map[String, Any](), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.getOrElse("") == "chicken")
    }

    it("Should throw an exception if the package is not among listed step packages") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse"), Some("com.acxiom.pipeline.fake"))))
      val thrown = intercept[IllegalArgumentException] {
        ReflectionUtils.processStep(step, pipeline, Map[String, Any]("string" -> "string"), pipelineContext)
      }
      assert(thrown.getMessage == "Package: [com.acxiom.pipeline.fake] was not found among step packages:" +
        " [com.acxiom.pipeline, com.acxiom.pipeline.steps]")
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
      val mc = ReflectionUtils.loadClass(className, Some(Map[String, Any]("test" -> true)))
      assert(mc.isInstanceOf[MockNoParams])
      assert(mc.asInstanceOf[MockNoParams].string == "no-constructor-string")

      val mc1 = ReflectionUtils.loadClass(className, None)
      assert(mc1.isInstanceOf[MockNoParams])
      assert(mc1.asInstanceOf[MockNoParams].string == "no-constructor-string")
    }

    it("Should instantiate default param constructor") {
      val className = "com.acxiom.pipeline.MockDefaultParam"
      val mc = ReflectionUtils.loadClass(className, Some(Map[String, Any]("test" -> true)))
      assert(mc.isInstanceOf[MockDefaultParam])
      assert(!mc.asInstanceOf[MockDefaultParam].getFlag)
      assert(mc.asInstanceOf[MockDefaultParam].getSecondParam == "none")

      val mc1  = ReflectionUtils.loadClass(className, Some(Map[String, Any]("flag" -> true)))
      assert(mc1.isInstanceOf[MockDefaultParam])
      assert(mc1.asInstanceOf[MockDefaultParam].getFlag)
      assert(mc1.asInstanceOf[MockDefaultParam].getSecondParam == "none")

      val mc2  = ReflectionUtils.loadClass(className, None)
      assert(mc2.isInstanceOf[MockDefaultParam])
      assert(!mc2.asInstanceOf[MockDefaultParam].getFlag)
      assert(mc2.asInstanceOf[MockDefaultParam].getSecondParam == "none")

      val mc3  = ReflectionUtils.loadClass(className, Some(Map[String, Any]("secondParam" -> "some")))
      assert(mc3.isInstanceOf[MockDefaultParam])
      assert(!mc3.asInstanceOf[MockDefaultParam].getFlag)
      assert(mc3.asInstanceOf[MockDefaultParam].getSecondParam == "some")
    }
  }

  describe("ReflectionUtils - extractField") {
    it("Should return None when field name is invalid") {
      assert(ReflectionUtils.extractField(MockObject("string", FIVE), "bob").asInstanceOf[Option[_]].isEmpty)
    }

    it("Should return None when entity is None") {
      assert(ReflectionUtils.extractField(None, "something").asInstanceOf[Option[_]].isEmpty)
    }

    it("Should return the option and not the value") {
      assert(ReflectionUtils.extractField(
       Some(MockParent(MockObject("string", FIVE, Some("string")))),
        "mock.opt", extractFromOption = false).asInstanceOf[Option[String]].getOrElse("bad") == "string")
    }
  }
}

case class MockParent(mock: MockObject)
case class MockObject(string: String, num: Int, opt: Option[String] = None)
