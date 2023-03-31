package com.acxiom.metalus.utils

import com.acxiom.metalus._
import com.acxiom.metalus.context.ContextManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.event.Level
import org.slf4j.{Logger, LoggerFactory}

import java.math.BigInteger
import java.util

class ReflectionUtilsTests extends AnyFunSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)
  }

  override def afterAll(): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).atLevel(Level.INFO)
  }

  describe("ReflectionUtil - processStep") {
    val globals = Map[String, Any]("validateStepParameterTypes" -> true)
    val stateInfo = PipelineStateKey("TestPipeline")
    val pipelineContext = PipelineContext(Some(globals), List(),
      Some(List("com.acxiom.metalus", "com.acxiom.metalus.steps")), PipelineStepMapper(),
      contextManager = new ContextManager(Map(), Map()),
      currentStateInfo = Some(stateInfo))
    it("Should process step function") {
      val step = PipelineStep(Some("TestStep"), engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("string" -> "string", "boolean" -> true),
        pipelineContext.setCurrentStateInfo(stateInfo.copy(stepId = step.id)))
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.getOrElse("") == "string")
      assert(response.namedReturns.isDefined)
      assert(response.namedReturns.get("boolean").asInstanceOf[Boolean])
    }

    it("Should process step function with non-PipelineStepResponse") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse"), Some("com.acxiom.metalus.steps"))))
      val response = ReflectionUtils.processStep(step, Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.getOrElse("") == "string")
    }

    it("Should process step with Option") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("string" -> "string", "boolean" -> Some(true), "opt" -> "Option"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.getOrElse("") == "string")
      assert(response.namedReturns.isDefined)
      assert(response.namedReturns.get("boolean").asInstanceOf[Boolean])
      assert(response.namedReturns.isDefined)
      assert(response.namedReturns.get("option").asInstanceOf[Option[String]].getOrElse("") == "Option")
    }

    it("Should process step with default value") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithDefaultValue"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get == "chicken")
    }

    it("Should process step with default value no option") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithDefaultValueNoOption"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("string" -> "string", "default" -> None), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get == "default chicken")
    }

    it("Should wrap values in a List, Seq, or Array if passing a single element to a collection") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithListParams"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("list" -> "l1", "seq" -> 1, "arrayList" -> new util.ArrayList()), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get == "Some(l1),Some(1),None")
    }

    it("Should handle scala.List prefix") {
      val ret = ReflectionUtils.loadClass("com.acxiom.metalus.utils.ClassWithOptionList", Some(Map[String, Any](
        "list" -> "moo",
        "optionList" -> Some(List[String]()),
        "optionList2" -> List[String]("moo2")
      )))
      assert(ret.isInstanceOf[ClassWithOptionList])
      val opList = ret.asInstanceOf[ClassWithOptionList]
      assert(opList.optionList.isDefined)
      assert(opList.list.headOption.contains("moo"))
      assert(opList.optionList2.getOrElse(List()).headOption.contains("moo2"))
    }

    it("Should unwrap an option passed to a non option param"){
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepWithListOfOptions"))))
      val response = ReflectionUtils.processStep(step,
        Map[String, Any]("s" -> Some(List(Some("s1"), Some("s2")))), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get == "s1,s2")
    }

    it("Should return an informative error if a step function is not found") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.typo"))))
      val thrown = intercept[IllegalArgumentException] {
        ReflectionUtils.processStep(step, Map[String, Any](), pipelineContext)
      }
      assert(thrown.getMessage == "typo is not a valid function!")
    }

    it("Should respect the validateStepParameterTypes global") {
      val step = PipelineStep(Some("chicken"), engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val thrown = intercept[ClassCastException] {
        ReflectionUtils.processStep(step, Map[String, Any]("string" -> 1), pipelineContext.setGlobal("validateStepParameterTypes", false))
      }
      val message = "java.lang.Integer cannot be cast to java.lang.String"
      assert(thrown.getMessage == message)
    }

    it("Should return an informative error if the parameter types do not match function params") {
      val step = PipelineStep(Some("chicken"), engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val thrown = intercept[PipelineException] {
        ReflectionUtils.processStep(step, Map[String, Any]("string" -> 1),
          pipelineContext.setCurrentStateInfo(pipelineContext.currentStateInfo.get.copy(stepId = step.id)))
      }
      val message = "Failed to map value [Some(1)] of type [Some(Integer)] to paramName [string] of" +
        " type [Option[String]] for method [mockStepFunctionWithOptionalGenericParams] in step [chicken] in pipeline [TestPipeline]"
      assert(thrown.getMessage == message)
    }

    it("should handle primitive types") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithPrimitives"))))
      val map = Map[String, Any]("i" -> 1,
        "l" -> 1L,
        "s" -> 1.toShort.asInstanceOf[java.lang.Short],
        "d" -> 1.0D,
        "f" -> 1.0F,
        "c" -> '1',
        "by" -> 1.toByte.asInstanceOf[java.lang.Byte],
        "a" -> "anyValue"
      )
      val response = ReflectionUtils.processStep(step, map, pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get == 1)
    }

    it("should handle Boxed types") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithBoxClasses"))))
      val map = Map[String, Any]("i" -> "1".toInt,
        "l" -> 1L,
        "s" -> 1.toShort.asInstanceOf[java.lang.Short],
        "d" -> 1.0D,
        "f" -> 1.0F,
        "c" -> '1',
        "by" -> 1.toByte.asInstanceOf[java.lang.Byte],
        "a" -> "anyValue"
      )
      val response = ReflectionUtils.processStep(step, map, pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get == 1)
    }

    it("Should respect the pkg setting on EngineMeta") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse"), Some("com.acxiom.metalus.steps"))))
      val response = ReflectionUtils.processStep(step, Map[String, Any]("string" -> "string"), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.getOrElse("") == "string")
    }

    it("Should inject None for missing parameters") {
      val step = PipelineStep(engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithOptionalGenericParams"))))
      val response = ReflectionUtils.processStep(step, Map[String, Any](), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.getOrElse("") == "chicken")
    }
  }

  describe("ReflectionUtils - loadClass") {
    it("Should instantiate a class") {
      val className = "com.acxiom.metalus.MockClass"
      val mc = ReflectionUtils.loadClass(className, Some(Map[String, Any]("string" -> "my_string")))
      assert(mc.isInstanceOf[MockClass])
      assert(mc.asInstanceOf[MockClass].string == "my_string")
    }

    it("Should instantiate a complex class") {
      val className = "com.acxiom.metalus.MockDriverSetup"
      val params = Map[String, Any]("parameters" -> Map[String, Any]("initialPipelineId" -> "pipelineId1"))
      val mockDriverSetup = ReflectionUtils.loadClass(className, Some(params))
      assert(mockDriverSetup.isInstanceOf[MockDriverSetup])
    }

    it("Should instantiate no param constructor") {
      val className = "com.acxiom.metalus.MockNoParams"
      val mc = ReflectionUtils.loadClass(className, Some(Map[String, Any]("test" -> true)))
      assert(mc.isInstanceOf[MockNoParams])
      assert(mc.asInstanceOf[MockNoParams].string == "no-constructor-string")

      val mc1 = ReflectionUtils.loadClass(className, None)
      assert(mc1.isInstanceOf[MockNoParams])
      assert(mc1.asInstanceOf[MockNoParams].string == "no-constructor-string")
    }

    it("Should instantiate default param constructor") {
      val className = "com.acxiom.metalus.MockDefaultParam"
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
      assert(ReflectionUtils.extractField(MockObject("string", Constants.FIVE), "bob").asInstanceOf[Option[_]].isEmpty)
    }

    it("Should execute methods without parameters") {
      val number = BigInt(3)
      assert(ReflectionUtils.extractField(number, "bigInteger").isInstanceOf[BigInteger])
    }

    it("Should return None when entity is None") {
      assert(ReflectionUtils.extractField(None, "something").asInstanceOf[Option[_]].isEmpty)
    }

    it("Should return the option and not the value") {
      assert(ReflectionUtils.extractField(
       Some(MockParent(MockObject("string", Constants.FIVE, Some("string")))),
        "mock.opt", extractFromOption = false).asInstanceOf[Option[String]].getOrElse("bad") == "string")
    }

    it("Should access array elements") {
      val list = List(1,2,3)
      val array = List(MockObject("moo", 1), MockObject("moo2", 2)).toArray
      val obj = Map("list" -> list, "array" -> array, "string" -> "moo")
      val listRes = ReflectionUtils.extractField(obj, "list[1]")
      assert(listRes == 2)
      val arrayRes = ReflectionUtils.extractField(obj, "array[0].string")
      assert(arrayRes == "moo")
      val stringRes = ReflectionUtils.extractField(obj, "string[1]")
      assert(stringRes == 'o')
      val badRes = ReflectionUtils.extractField(obj, "array[3].string")
      assert(badRes == None)
    }
  }
}

case class MockParent(mock: MockObject)
case class MockObject(string: String, num: Int, opt: Option[String] = None)
class ClassWithOptionList(val list: List[String], val optionList: Option[scala.List[Any]], val optionList2: Option[scala.List[Any]]) {
}
