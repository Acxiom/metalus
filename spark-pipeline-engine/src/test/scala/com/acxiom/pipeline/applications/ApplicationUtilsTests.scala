package com.acxiom.pipeline.applications

import com.acxiom.pipeline.{PipelineListener, PipelineSecurityManager, PipelineStepMapper}
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.scalatest.FunSpec

import scala.io.Source

class ApplicationUtilsTests extends FunSpec {
  describe("ApplicationUtils - Parsing") {
    // Load the application for all the tests
    val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/application-test.json")).mkString)
    it("Should create an execution plan") {
      val sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
        .setMaster("local[2]")
      val executionPlan = ApplicationUtils.createExecutionPlan(application, Some(Map[String, Any]("root" -> true)), sparkConf)
      assert(executionPlan.length == 2)
      // First execution
      val execution1 = executionPlan.head
      val ctx1 = execution1.pipelineContext
      // Ensure the id is set properly
      assert(execution1.id == "0")
      // Use the global listener
      assert(ctx1.pipelineListener.get.isInstanceOf[TestPipelineListener])
      assert(ctx1.pipelineListener.get.asInstanceOf[TestPipelineListener].name == "Test Pipeline Listener")
      // Use a custom security manager
      assert(ctx1.security.asInstanceOf[TestPipelineSecurityManager].name == "Sub Security Manager")
      // Use the global pipeline parameters
      assert(ctx1.parameters.parameters.length == 1)
      assert(ctx1.parameters.parameters.head.parameters.getOrElse("fred", "") == "johnson")
      // Use the global step mapper
      assert(ctx1.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Test Step Mapper")
      // Validate the correct pipeline is set
      assert(execution1.pipelines.head.name.getOrElse("") == "Pipeline 1")
      // Ensure no parents
      assert(execution1.parents.isEmpty)
      // Verify the globals object was properly constructed
      val globals = ctx1.globals.get
      assert(globals.size == 5)
      assert(globals.contains("root"))
      assert(globals("root").asInstanceOf[Boolean])
      assert(globals.contains("number"))
      assert(globals("number").asInstanceOf[BigInt] == 2)
      assert(globals.contains("float"))
      assert(globals("float").asInstanceOf[Double] == 3.5)
      assert(globals.contains("string"))
      assert(globals("string").asInstanceOf[String] == "sub string")
      assert(globals.contains("mappedObject"))
      val subGlobalObject = globals("mappedObject").asInstanceOf[TestGlobalObject]
      assert(subGlobalObject.name.getOrElse("") == "Execution Mapped Object")
      assert(subGlobalObject.subObjects.isDefined)
      assert(subGlobalObject.subObjects.get.length == 3)
      assert(subGlobalObject.subObjects.get.head.name.getOrElse("") == "Sub Object 1a")
      assert(subGlobalObject.subObjects.get(1).name.getOrElse("") == "Sub Object 2a")
      assert(subGlobalObject.subObjects.get(2).name.getOrElse("") == "Sub Object 3")

      // Second execution
      val execution2 = executionPlan(1)
      val ctx2 = execution2.pipelineContext
      // Ensure the id is set properly
      assert(execution2.id == "1")
      // Second execution uses the listener defined for it
      assert(ctx2.pipelineListener.get.isInstanceOf[TestPipelineListener])
      assert(ctx2.pipelineListener.get.asInstanceOf[TestPipelineListener].name == "Sub Pipeline Listener")
      // Use a global security manager
      assert(ctx2.security.asInstanceOf[TestPipelineSecurityManager].name == "Test Security Manager")
      // Use the custom pipeline parameters
      assert(ctx2.parameters.parameters.length == 1)
      assert(ctx2.parameters.parameters.head.parameters.getOrElse("howard", "") == "johnson")
      // Use the custom step mapper
      assert(ctx2.parameterMapper.asInstanceOf[TestPipelineStepMapper].name == "Sub Step Mapper")
      // Validate the correct pipeline is set
      assert(execution2.pipelines.head.name.getOrElse("") == "Pipeline 2")
      // Ensure the correct parent
      assert(execution2.parents.isDefined)
      assert(execution2.parents.get.head == "0")
      // Verify the globals object was properly constructed
      val globals1 = ctx2.globals.get
      assert(globals1.size == 5)
      assert(globals1.contains("root"))
      assert(globals1("root").asInstanceOf[Boolean])
      assert(globals1.contains("number"))
      assert(globals1("number").asInstanceOf[BigInt] == 1)
      assert(globals1.contains("float"))
      assert(globals1("float").asInstanceOf[Double] == 1.5)
      assert(globals1.contains("string"))
      assert(globals1("string").asInstanceOf[String] == "some string")
      assert(globals1.contains("mappedObject"))
      val subGlobalObject1 = globals1("mappedObject").asInstanceOf[TestGlobalObject]
      assert(subGlobalObject1.name.getOrElse("") == "Global Mapped Object")
      assert(subGlobalObject1.subObjects.isDefined)
      assert(subGlobalObject1.subObjects.get.length == 2)
      assert(subGlobalObject1.subObjects.get.head.name.getOrElse("") == "Sub Object 1")
      assert(subGlobalObject1.subObjects.get(1).name.getOrElse("") == "Sub Object 2")
      // TODO Test pipeline parameters
    }
  }
}

case class TestPipelineListener(name: String) extends PipelineListener
case class TestPipelineSecurityManager(name: String) extends PipelineSecurityManager
case class TestPipelineStepMapper(name: String) extends PipelineStepMapper
case class TestGlobalObject(name: Option[String], subObjects: Option[List[TestSubGlobalObject]])
case class TestSubGlobalObject(name: Option[String])