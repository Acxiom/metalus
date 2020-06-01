package com.acxiom.pipeline.utils

import java.io.File
import java.util

import com.acxiom.pipeline.{PipelineStepResponse, _}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class ReflectionUtilsTests extends FunSpec with BeforeAndAfterAll {
  private val FIVE = 5

  override def beforeAll() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = new SparkConf()
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      ",org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
        "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
        "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

    SparkTestHelper.sparkSession = SparkSession.builder().config(SparkTestHelper.sparkConf).getOrCreate()

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  override def afterAll() {
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

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

    it("Should process step with default value no option") {
      val step = PipelineStep(None, None, None, None, None,
        Some(EngineMeta(Some("MockStepObject.mockStepFunctionWithDefaultValueNoOption"))))
      val response = ReflectionUtils.processStep(step, pipeline,
        Map[String, Any]("string" -> "string", "default" -> None), pipelineContext)
      assert(response.isInstanceOf[PipelineStepResponse])
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(response.asInstanceOf[PipelineStepResponse].primaryReturn.get == "default chicken")
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

    it("Should handle scala.List prefix") {
      val ret = ReflectionUtils.loadClass("com.acxiom.pipeline.utils.ClassWithOptionList", Some(Map[String, Any](
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

    it("Should execute methods without parameters") {
      val spark = SparkTestHelper.sparkSession
      import spark.implicits._
      val df = Seq((1, "silkie"), (2, "polish"), (3, "buttercup")).toDF("id", "chicken")
      assert(!ReflectionUtils.extractField(df, "isEmpty", applyMethod = Some(true)).asInstanceOf[Boolean])
      assert(ReflectionUtils.extractField(df, "count", applyMethod = Some(true)) == 3)
    }

    it("Should fail on methods if method extraction is disabled") {
      val thrown = intercept[IllegalArgumentException] {
        ReflectionUtils.extractField(List(), "isEmpty")
      }
      val expected = "Unable to extract: [Nil.isEmpty]. isEmpty is a method." +
        " To enable method extraction, set the global [extractMethodsEnabled] to true."
      assert(thrown.getMessage == expected)
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
class ClassWithOptionList(val list: List[String], val optionList: Option[scala.List[Any]], val optionList2: Option[scala.List[Any]]) {
}
