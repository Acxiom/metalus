package com.acxiom.pipeline.applications

import com.acxiom.pipeline._
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

/**
  * Provides a set of utility functions for working with Application metadata
  */
object ApplicationUtils {
  /**
    * This function will parse an Application from the provided JSON.
    *
    * @param json The json string representing an Application.
    * @return An Application object.
    */
  def parseApplication(json: String): Application = {
    DriverUtils.parseJson(json, "com.acxiom.pipeline.applications.Application").asInstanceOf[Application]
  }

  /**
    * This function will convert the Application into an execution plan supplying the globals (if provided) to each
    * PipelineContext. This function will create a SparkSession using the provided SparkConf.
    *
    * @param application      The Application to use to generate the execution plan.
    * @param globals          An optional set of globals to use in each PipelineContext
    * @param sparkConf        The SparkConf to use
    * @param pipelineListener An optional PipelineListener. This may be overridden by the application.
    * @return An execution plan.
    */
  def createExecutionPlan(application: Application,
                          globals: Option[Map[String, Any]],
                          sparkConf: SparkConf,
                          pipelineListener: PipelineListener = DefaultPipelineListener()): List[PipelineExecution] = {
    // Create the SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // Create the default globals
    val rootGlobals = globals.getOrElse(Map[String, Any]())
    val defaultGlobals = generateGlobals(application.globals, rootGlobals, Some(rootGlobals))
    val globalListener = generatePipelineListener(application.pipelineListener, Some(pipelineListener))
    val globalSecurityManager = generateSecurityManager(application.securityManager, Some(PipelineSecurityManager()))
    val globalStepMapper = generateStepMapper(application.stepMapper, Some(PipelineStepMapper()))
    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(PipelineParameters()))
    // Generate the execution plan
    application.executions.get.map(execution => {
      val ctx = PipelineContext(Some(sparkConf),
        Some(sparkSession),
        generateGlobals(execution.globals, rootGlobals, defaultGlobals),
        generateSecurityManager(execution.securityManager, globalSecurityManager).get,
        generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get,
        application.stepPackages,
        generateStepMapper(execution.stepMapper, globalStepMapper).get,
        generatePipelineListener(execution.pipelineListener, globalListener),
        Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
      PipelineExecution(execution.id.getOrElse(""), execution.pipelines.get, execution.initialPipelineId, ctx, execution.parents)
    })
  }

  private def generatePipelineListener(pipelineListenerInfo: Option[ClassInfo],
                                       pipelineListener: Option[PipelineListener]): Option[PipelineListener] = {
    if (pipelineListenerInfo.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineListenerInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineListener"),
        pipelineListenerInfo.get.parameters).asInstanceOf[PipelineListener])
    } else {
      pipelineListener
    }
  }

  private def generateSecurityManager(securityManagerInfo: Option[ClassInfo],
                                      securityManager: Option[PipelineSecurityManager]): Option[PipelineSecurityManager] = {
    if (securityManagerInfo.isDefined) {
      Some(ReflectionUtils.loadClass(securityManagerInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineSecurityManager"),
        securityManagerInfo.get.parameters).asInstanceOf[PipelineSecurityManager])
    } else {
      securityManager
    }
  }

  private def generateStepMapper(stepMapperInfo: Option[ClassInfo],
                                 stepMapper: Option[PipelineStepMapper]): Option[PipelineStepMapper] = {
    if (stepMapperInfo.isDefined) {
      Some(ReflectionUtils.loadClass(stepMapperInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineStepMapper"),
        stepMapperInfo.get.parameters).asInstanceOf[PipelineStepMapper])
    } else {
      stepMapper
    }
  }

  private def generatePipelineParameters(pipelineParameters: Option[PipelineParameters],
                                         defaultPipelineParameters: Option[PipelineParameters]): Option[PipelineParameters] = {
    if (pipelineParameters.isDefined) {
      pipelineParameters
    } else {
      defaultPipelineParameters
    }
  }

  private def generateGlobals(globals: Option[Map[String, Any]],
                              rootGlobals: Map[String, Any],
                              defaultGlobals: Option[Map[String, Any]]): Option[Map[String, Any]] = {
    if (globals.isEmpty) {
      defaultGlobals
    } else {
      implicit val formats: Formats = DefaultFormats
      val baseGlobals = globals.get
      Some(baseGlobals.foldLeft(rootGlobals)((rootMap, entry) => {
        entry._2 match {
          case map: Map[String, Any] if map.contains("className") =>
            val obj = DriverUtils.parseJson(Serialization.write(map("object").asInstanceOf[Map[String, Any]]), map("className").asInstanceOf[String])
            rootMap + (entry._1 -> obj)
          case _ => rootMap + (entry._1 -> entry._2)
        }
      }))
    }
  }
}
