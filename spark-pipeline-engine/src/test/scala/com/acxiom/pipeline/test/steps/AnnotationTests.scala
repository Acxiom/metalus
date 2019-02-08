package com.acxiom.pipeline.test.steps

import java.nio.file.Files

import com.acxiom.pipeline.annotations.{PipelineStepsDefintion, StepFunction, StepMetaDataExtractor, StepObject}
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.FunSpec
import org.json4s.native.JsonMethods.parse
import scala.io.Source

class AnnotationTests extends FunSpec {
  describe("Step Annotation Tests") {
    it("Should find step object in provided package") {
      val outputFile = Files.createTempFile("stepPackageOutput", ".json")
      outputFile.toFile.deleteOnExit()
      StepMetaDataExtractor.main(Array(
        "--stepPackages", "com.acxiom.pipeline.test.steps, com.acxiom.pipeline.nosteps",
        "--outputFile", outputFile.toFile.getAbsolutePath))
      val json = Source.fromFile(outputFile.toFile).mkString
      assert(Option(json).isDefined)
      implicit val formats: Formats = DefaultFormats
      val definition = parse(json).extract[PipelineStepsDefintion]
      assert(definition.pkgs.lengthCompare(1) == 0)
      assert(definition.pkgs.head == "com.acxiom.pipeline.test.steps")
      assert(definition.steps.lengthCompare(2) == 0)
      val step1 = definition.steps.find(step => step.id == "12345")
      assert(step1.isDefined)
      assert(step1.get.displayName == "No Parameter Step Function")
      assert(step1.get.description == "No description")
      assert(step1.get.`type` == "Pipeline")
      assert(step1.get.params.isEmpty)
      assert(step1.get.engineMeta.spark.isDefined)
      assert(step1.get.engineMeta.spark.get == "AnnotatedStepObject.stepFunction")

      val step2 = definition.steps.find(step => step.id == "54321")
      assert(Option(step2).isDefined)
      assert(step2.get.displayName == "Parameter Step Function")
      assert(step2.get.description == "No description")
      assert(step2.get.`type` == "Pipeline")
      assert(step2.get.params.nonEmpty)
      assert(step2.get.params.lengthCompare(2) == 0)
      val param1 = step2.get.params.find(p => p.name == "id")
      assert(param1.isDefined)
      assert(param1.get.`type` == "text")
      val param2 = step2.get.params.find(p => p.name == "num")
      assert(param2.isDefined)
      assert(param2.get.`type` == "number")
      assert(step2.get.engineMeta.spark.isDefined)
      assert(step2.get.engineMeta.spark.get == "AnotherStepObject.stepFunctionWithParams")
    }
  }
}

@StepObject
object AnnotatedStepObject {
  @StepFunction("12345", "No Parameter Step Function", "No description", "Pipeline")
  def stepFunction(): Unit = {}
  def stepFunction(num: Integer): Unit = {}
  def nonStepObject(): Unit = {}
}

@StepObject
object AnotherStepObject {
  @StepFunction("54321", "Parameter Step Function", "No description", "Pipeline")
  def stepFunctionWithParams(id: String, num: Integer): Unit = {}
}

@StepObject
object SomeStepObject {
  def nonStepObject(): Unit = {}
}
