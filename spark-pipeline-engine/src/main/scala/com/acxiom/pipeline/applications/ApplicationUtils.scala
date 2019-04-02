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
                          pipelineListener: PipelineListener = DefaultPipelineListener(),
                          enableHiveSupport: Boolean = false): List[PipelineExecution] = {
    // Create the SparkSession
    val sparkSession = if (enableHiveSupport) {
      SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    } else {
      SparkSession.builder().config(sparkConf).getOrCreate()
    }
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
        generateGlobals(execution.globals, rootGlobals, defaultGlobals, execution.mergeGlobals),
        generateSecurityManager(execution.securityManager, globalSecurityManager).get,
        generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get,
        application.stepPackages,
        generateStepMapper(execution.stepMapper, globalStepMapper).get,
        generatePipelineListener(execution.pipelineListener, globalListener),
        Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
      // Extracting pipelines
      val pipelines = generatePipelines(execution, application)
      PipelineExecution(execution.id.getOrElse(""), pipelines, execution.initialPipelineId, ctx, execution.parents)
    })
  }

  /**
    * Utility method that resets the state on the PipelineExecution.
    *
    * @param application       The Application configuration
    * @param rootGlobals       The initial set of globals
    * @param execution         The execution configuration
    * @param pipelineExecution The PipelineExecution that needs to be refreshed
    * @return An updated PipelineExecution
    */
  def refreshPipelineExecution(application: Application,
                               rootGlobals: Option[Map[String, Any]],
                               execution: Execution,
                               pipelineExecution: PipelineExecution): PipelineExecution = {
    val defaultGlobals = generateGlobals(application.globals, rootGlobals.get, rootGlobals)
    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(PipelineParameters()))
    val ctx = pipelineExecution.pipelineContext
      .copy(globals = generateGlobals(execution.globals, rootGlobals.get, defaultGlobals, execution.mergeGlobals))
      .copy(parameters = generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get)
    pipelineExecution.asInstanceOf[DefaultPipelineExecution].copy(pipelineContext = ctx)
  }

  private def generatePipelines(execution: Execution, application: Application): List[Pipeline] = {
    val executionPipelines = execution.pipelines.getOrElse(List[DefaultPipeline]())
    val pipelineIds = execution.pipelineIds.getOrElse(List[String]())
    val applicationPipelines = application.pipelines.getOrElse(List[DefaultPipeline]())

    if (pipelineIds.nonEmpty) {
      val filteredExecutionPipelines = executionPipelines.filter(p => pipelineIds.contains(p.id.getOrElse("")))
      filteredExecutionPipelines ++ applicationPipelines
        .filter(p => {
          !filteredExecutionPipelines.exists(e => e.id.getOrElse("") == p.id.getOrElse("")) && pipelineIds.contains(p.id.get)
        })
    } else if (executionPipelines.nonEmpty) {
      executionPipelines
    } else if (applicationPipelines.nonEmpty) {
      applicationPipelines
    } else {
      throw new IllegalArgumentException("Either execution pipelines, pipelineIds or application pipelines must be provided for an execution")
    }
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
                              defaultGlobals: Option[Map[String, Any]],
                              merge: Boolean = false): Option[Map[String, Any]] = {
    if (globals.isEmpty) {
      defaultGlobals
    } else {
      implicit val formats: Formats = DefaultFormats
      val baseGlobals = globals.get
      val result = baseGlobals.foldLeft(rootGlobals)((rootMap, entry) => {
        entry._2 match {
          case map: Map[String, Any] if map.contains("className") =>
            val obj = DriverUtils.parseJson(Serialization.write(map("object").asInstanceOf[Map[String, Any]]), map("className").asInstanceOf[String])
            rootMap + (entry._1 -> obj)
          case _ => rootMap + (entry._1 -> entry._2)
        }
      })
      Some(if (merge) {
        defaultGlobals.getOrElse(Map[String, Any]()) ++ result
      } else {
        result
      })
    }
  }
}
