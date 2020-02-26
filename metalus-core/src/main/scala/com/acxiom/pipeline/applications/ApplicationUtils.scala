package com.acxiom.pipeline.applications

import com.acxiom.pipeline._
import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

/**
 * Provides a set of utility functions for working with Application metadata
 */
object ApplicationUtils {
  private val logger = Logger.getLogger(getClass)

  /**
   * This function will parse an Application from the provided JSON.
   *
   * @param json The json string representing an Application.
   * @return An Application object.
   */
  def parseApplication(json: String): Application = {
    // See if this is an application response
    if (json.indexOf("application\"") > -1 && json.indexOf("application") < 15) {
      DriverUtils.parseJson(json, "com.acxiom.pipeline.applications.ApplicationResponse").asInstanceOf[ApplicationResponse].application
    } else {
      DriverUtils.parseJson(json, "com.acxiom.pipeline.applications.Application").asInstanceOf[Application]
    }
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
                          pipelineListener: PipelineListener = PipelineListener(),
                          enableHiveSupport: Boolean = false,
                          parquetDictionaryEnabled: Boolean = true): List[PipelineExecution] = {
    // Create the SparkSession
    val sparkSession = if (enableHiveSupport) {
      SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    } else {
      SparkSession.builder().config(sparkConf).getOrCreate()
    }
    logger.info(s"setting parquet dictionary enabled to ${parquetDictionaryEnabled.toString}")
    sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.dictionary", parquetDictionaryEnabled.toString)
    // Create the default globals
    val rootGlobals = globals.getOrElse(Map[String, Any]())
    val defaultGlobals = generateGlobals(application.globals, rootGlobals, Some(rootGlobals))
    val globalListener = generatePipelineListener(application.pipelineListener, Some(pipelineListener))
    val globalSecurityManager = generateSecurityManager(application.securityManager, Some(PipelineSecurityManager()))
    val globalStepMapper = generateStepMapper(application.stepMapper, Some(PipelineStepMapper()))
    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(PipelineParameters()))
    val pipelineManager = generatePipelineManager(application.pipelineManager,
      Some(PipelineManager(application.pipelines.getOrElse(List[DefaultPipeline]())))).get
    generateSparkListeners(application.sparkListeners).getOrElse(List()).foreach(sparkSession.sparkContext.addSparkListener)
    if (globalListener.isDefined && globalListener.get.isInstanceOf[SparkListener]) {
      sparkSession.sparkContext.addSparkListener(globalListener.get.asInstanceOf[SparkListener])
    }
    registerSparkUDFs(defaultGlobals, sparkSession, application.sparkUdfs)
    // Generate the execution plan
    application.executions.get.map(execution => {
      val pipelineListener = generatePipelineListener(execution.pipelineListener, globalListener)
      if (execution.pipelineListener.isDefined && pipelineListener.isDefined && pipelineListener.get.isInstanceOf[SparkListener]) {
        sparkSession.sparkContext.addSparkListener(pipelineListener.get.asInstanceOf[SparkListener])
      }
      generateSparkListeners(execution.sparkListeners).getOrElse(List()).foreach(sparkSession.sparkContext.addSparkListener)
      // Extracting pipelines
      val ctx = PipelineContext(Some(sparkConf),
        Some(sparkSession),
        generateGlobals(execution.globals, rootGlobals, defaultGlobals, execution.mergeGlobals.getOrElse(false)),
        generateSecurityManager(execution.securityManager, globalSecurityManager).get,
        generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get,
        application.stepPackages,
        generateStepMapper(execution.stepMapper, globalStepMapper).get,
        pipelineListener,
        Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")),
        ExecutionAudit("root", AuditType.EXECUTION, Map[String, Any](), System.currentTimeMillis()),
        pipelineManager)
      PipelineExecution(execution.id.getOrElse(""),
        generatePipelines(execution, application, pipelineManager), execution.initialPipelineId, ctx, execution.parents)
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
      .copy(globals = generateGlobals(execution.globals, rootGlobals.get, defaultGlobals, execution.mergeGlobals.getOrElse(false)))
      .copy(parameters = generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get)
    pipelineExecution.asInstanceOf[DefaultPipelineExecution].copy(pipelineContext = ctx)
  }

  private def generatePipelines(execution: Execution, application: Application, pipelineManager: PipelineManager): List[Pipeline] = {
    val executionPipelines = execution.pipelines.getOrElse(List[Pipeline]())
    val pipelineIds = execution.pipelineIds.getOrElse(List[String]())
    val applicationPipelines = application.pipelines.getOrElse(List[Pipeline]())

    if (pipelineIds.nonEmpty) {
      // Start with any pipelines that are part of the execution and listed in the pipelineIds
      val filteredExecutionPipelines = executionPipelines.filter(p => pipelineIds.contains(p.id.getOrElse("")))
      // Get the remaining pipelines listed in pipelineIds
      pipelineIds.filter(id => !filteredExecutionPipelines.exists(_.id.getOrElse("") == id))
        .foldLeft(filteredExecutionPipelines)((pipelines, pipelineId) => {
        val pipeline = applicationPipelines.find(p => p.id.getOrElse("") == pipelineId)
        if (pipeline.isDefined) {
          pipelines :+ pipeline.get
        } else {
          val lookupPipeline = pipelineManager.getPipeline(pipelineId)
          if (lookupPipeline.isDefined) {
            pipelines :+ lookupPipeline.get
          } else {
            pipelines
          }
        }
      })
    } else if (executionPipelines.nonEmpty) {
      executionPipelines
    } else if (applicationPipelines.nonEmpty) {
      applicationPipelines
    } else {
      throw new IllegalArgumentException("Either execution pipelines, pipelineIds or application pipelines must be provided for an execution")
    }
  }

  private def generatePipelineManager(pipelineManagerInfo: Option[ClassInfo],
                                      pipelineManager: Option[PipelineManager]): Option[PipelineManager] = {
    if (pipelineManagerInfo.isDefined && pipelineManagerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineManagerInfo.get.className.getOrElse("com.acxiom.pipeline.CachedPipelineManager"),
        Some(parseParameters(pipelineManagerInfo.get))).asInstanceOf[PipelineManager])
    } else {
      pipelineManager
    }
  }

  private def generatePipelineListener(pipelineListenerInfo: Option[ClassInfo],
                                       pipelineListener: Option[PipelineListener]): Option[PipelineListener] = {
    if (pipelineListenerInfo.isDefined && pipelineListenerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineListenerInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineListener"),
        Some(parseParameters(pipelineListenerInfo.get))).asInstanceOf[PipelineListener])
    } else {
      pipelineListener
    }
  }

  private def generateSparkListeners(sparkListenerInfos: Option[List[ClassInfo]]): Option[List[SparkListener]] = {
    if (sparkListenerInfos.isDefined && sparkListenerInfos.get.nonEmpty) {
      Some(sparkListenerInfos.get.flatMap { info =>
        if (info.className.isDefined) {
          Some(ReflectionUtils.loadClass(info.className.get, Some(parseParameters(info))).asInstanceOf[SparkListener])
        } else {
          None
        }
      })
    } else {
      None
    }
  }

  private def generateSecurityManager(securityManagerInfo: Option[ClassInfo],
                                      securityManager: Option[PipelineSecurityManager]): Option[PipelineSecurityManager] = {
    if (securityManagerInfo.isDefined && securityManagerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(securityManagerInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineSecurityManager"),
        Some(parseParameters(securityManagerInfo.get))).asInstanceOf[PipelineSecurityManager])
    } else {
      securityManager
    }
  }

  private def generateStepMapper(stepMapperInfo: Option[ClassInfo],
                                 stepMapper: Option[PipelineStepMapper]): Option[PipelineStepMapper] = {
    if (stepMapperInfo.isDefined && stepMapperInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(stepMapperInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineStepMapper"),
        Some(parseParameters(stepMapperInfo.get))).asInstanceOf[PipelineStepMapper])
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

  private def registerSparkUDFs(globals: Option[Map[String, Any]], sparkSession: SparkSession, sparkUDFs: Option[List[ClassInfo]]): Unit = {
    if (sparkUDFs.isDefined && sparkUDFs.get.nonEmpty) {
      sparkUDFs.get.flatMap { info =>
        if (info.className.isDefined) {
          Some(ReflectionUtils.loadClass(info.className.get, Some(parseParameters(info))).asInstanceOf[PipelineUDF])
        } else {
          None
        }
      }.foreach(udf => udf.register(sparkSession, globals.getOrElse(Map())))
    }
  }

  private def generateGlobals(globals: Option[Map[String, Any]],
                              rootGlobals: Map[String, Any],
                              defaultGlobals: Option[Map[String, Any]],
                              merge: Boolean = false): Option[Map[String, Any]] = {
    if (globals.isEmpty) {
      defaultGlobals
    } else {
      val baseGlobals = globals.get
      val result = baseGlobals.foldLeft(rootGlobals)((rootMap, entry) => parseValue(rootMap, entry._1, entry._2))
      Some(if (merge) {
        defaultGlobals.getOrElse(Map[String, Any]()) ++ result
      } else {
        result
      })
    }
  }

  private def parseParameters(classInfo: ClassInfo): Map[String, Any] = {
    classInfo.parameters.getOrElse(Map[String, Any]())
      .foldLeft(Map[String, Any]())((rootMap, entry) => parseValue(rootMap, entry._1, entry._2))
  }

  private def parseValue(rootMap: Map[String, Any], key: String, value: Any) = {
    implicit val formats: Formats = DefaultFormats
    value match {
      case map: Map[String, Any] if map.contains("className") =>
        val obj = DriverUtils.parseJson(Serialization.write(map("object").asInstanceOf[Map[String, Any]]), map("className").asInstanceOf[String])
        rootMap + (key -> obj)
      case listMap: List[Any] =>
        val obj = listMap.map {
          case m: Map[String, Any] =>
            if (m.contains("className")) {
              DriverUtils.parseJson(Serialization.write(m("object").asInstanceOf[Map[String, Any]]), m("className").asInstanceOf[String])
            } else {
              m
            }
          case any => any
        }
        rootMap + (key -> obj)
      case _ => rootMap + (key -> value)
    }
  }
}
