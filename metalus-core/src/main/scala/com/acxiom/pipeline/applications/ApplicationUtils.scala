package com.acxiom.pipeline.applications

import com.acxiom.pipeline._
import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.json4s.ext.{EnumNameSerializer, EnumSerializer}
import org.json4s.native.Serialization
import org.json4s.{CustomSerializer, DefaultFormats, Formats}

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
    implicit val formats: Formats = DefaultFormats
    if (json.indexOf("application\"") > -1 && json.indexOf("application") < 15) {
      DriverUtils.parseJson(json, "com.acxiom.pipeline.applications.ApplicationResponse").asInstanceOf[ApplicationResponse].application
    } else {
      DriverUtils.parseJson(json, "com.acxiom.pipeline.applications.Application").asInstanceOf[Application]
    }
  }

  /**
   * Build a json4s Formats object using the ClassInfo objects in json4sSerializers. If json4sSerializers is not
   * provided, the Formats object will only use DefaultFormats.
   *
   * @param json4sSerializers Contains ClassInfo objects for custom serializers and enum serializers.
   * @return A json4s Formats object.
   */
  def getJson4sFormats(json4sSerializers: Option[Json4sSerializers]): Formats = {
    json4sSerializers.map { j =>
      val enumNames = j.enumNameSerializers.map(_.map(ci => new EnumNameSerializer(ReflectionUtils.loadEnumeration(ci.className.getOrElse("")))))
        .getOrElse(List())
      val enumIds = j.enumIdSerializers.map(_.map(ci => new EnumSerializer(ReflectionUtils.loadEnumeration(ci.className.getOrElse("")))))
        .getOrElse(List())
      val customSerializers = j.customSerializers.map(_.map { ci =>
        ReflectionUtils.loadClass(ci.className.getOrElse(""), ci.parameters).asInstanceOf[CustomSerializer[_]]
      }).getOrElse(List())
      (customSerializers ++ enumNames ++ enumIds).foldLeft(DefaultFormats: Formats) { (formats, custom) =>
        formats + custom
      }
    }.getOrElse(DefaultFormats)
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
  //noinspection ScalaStyle
  def createExecutionPlan(application: Application, globals: Option[Map[String, Any]], sparkConf: SparkConf,
                          pipelineListener: PipelineListener = PipelineListener(),
                          applicationTriggers: ApplicationTriggers = ApplicationTriggers(),
                          credentialProvider: Option[CredentialProvider] = None): List[PipelineExecution] = {
    logger.info("Building Execution Plan")
    val sparkSession = if (applicationTriggers.enableHiveSupport) { // Create the SparkSession
      SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    } else {
      SparkSession.builder().config(sparkConf).getOrCreate()
    }
    logger.info(s"Setting parquet dictionary enabled to ${applicationTriggers.parquetDictionaryEnabled.toString}")
    sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.dictionary", applicationTriggers.parquetDictionaryEnabled.toString)
    implicit val formats: Formats = getJson4sFormats(application.json4sSerializers)
    val globalStepMapper = generateStepMapper(application.stepMapper, Some(PipelineStepMapper()),
      applicationTriggers.validateArgumentTypes, credentialProvider)
    val rootGlobals = globals.getOrElse(Map[String, Any]()) // Create the default globals
    val globalListener = generatePipelineListener(application.pipelineListener, Some(pipelineListener),
      applicationTriggers.validateArgumentTypes, credentialProvider)
    val globalSecurityManager = generateSecurityManager(application.securityManager, Some(PipelineSecurityManager()),
      applicationTriggers.validateArgumentTypes, credentialProvider)
    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(PipelineParameters()))
    val pipelineManager = generatePipelineManager(application.pipelineManager,
      Some(PipelineManager(application.pipelines.getOrElse(List[DefaultPipeline]()))),
      applicationTriggers.validateArgumentTypes, credentialProvider).get
    val initialContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(rootGlobals), globalSecurityManager.get,
      globalPipelineParameters.get, application.stepPackages, globalStepMapper.get, globalListener,
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")),
      ExecutionAudit("root", AuditType.EXECUTION, Map[String, Any](), System.currentTimeMillis()), pipelineManager,
      credentialProvider, Some(formats))
    val defaultGlobals = generateGlobals(application.globals, rootGlobals , Some(rootGlobals), initialContext)
    generateSparkListeners(application.sparkListeners,
      applicationTriggers.validateArgumentTypes, credentialProvider).getOrElse(List()).foreach(sparkSession.sparkContext.addSparkListener)
    addSparkListener(globalListener, sparkSession)
    registerSparkUDFs(defaultGlobals, sparkSession, application.sparkUdfs, applicationTriggers.validateArgumentTypes, credentialProvider)
    application.executions.get.map(execution => { // Generate the execution plan
      val pipelineListener = generatePipelineListener(execution.pipelineListener, globalListener,
        applicationTriggers.validateArgumentTypes, credentialProvider)
      if (execution.pipelineListener.isDefined) {
        addSparkListener(pipelineListener, sparkSession)
      }
      generateSparkListeners(execution.sparkListeners,
        applicationTriggers.validateArgumentTypes, credentialProvider).getOrElse(List()).foreach(sparkSession.sparkContext.addSparkListener)
      val stepMapper = generateStepMapper(execution.stepMapper, globalStepMapper, applicationTriggers.validateArgumentTypes,
        credentialProvider).get
      // Extracting pipelines
      val ctx = initialContext.copy(
        globals = generateGlobals(execution.globals, rootGlobals, defaultGlobals, initialContext, execution.mergeGlobals.getOrElse(false)),
        security = generateSecurityManager(execution.securityManager, globalSecurityManager,
          applicationTriggers.validateArgumentTypes, credentialProvider).get,
        parameters = generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get,
        parameterMapper = stepMapper,
        pipelineListener = pipelineListener
      )
      PipelineExecution(execution.id.getOrElse(""),
        generatePipelines(execution, application, pipelineManager),
        execution.initialPipelineId,
        ctx,
        execution.parents,
        populatePipelines(execution.evaluationPipelines.getOrElse(List()),
          execution.evaluationPipelineIds.getOrElse(List()), application.pipelines.getOrElse(List()), pipelineManager, allowEmpty = true),
        execution.forkByValue,
        execution.executionType.getOrElse("pipeline"))
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
    implicit val formats: Formats = getJson4sFormats(application.json4sSerializers)
    val initialContext = pipelineExecution.pipelineContext.copy(globals = rootGlobals)
    val defaultGlobals = generateGlobals(application.globals, rootGlobals.get, rootGlobals, initialContext)
    val globalPipelineParameters = generatePipelineParameters(application.pipelineParameters, Some(PipelineParameters()))
    val ctx = pipelineExecution.pipelineContext
      .copy(globals = generateGlobals(execution.globals, rootGlobals.get, defaultGlobals,
        initialContext, execution.mergeGlobals.getOrElse(false)))
      .copy(parameters = generatePipelineParameters(execution.pipelineParameters, globalPipelineParameters).get)
    pipelineExecution.asInstanceOf[DefaultPipelineExecution].copy(pipelineContext = ctx)
  }

  private def generatePipelines(execution: Execution, application: Application, pipelineManager: PipelineManager): List[Pipeline] = {
    val executionPipelines = execution.pipelines.getOrElse(List[Pipeline]())
    val pipelineIds = execution.pipelineIds.getOrElse(List[String]())
    val applicationPipelines = application.pipelines.getOrElse(List[Pipeline]())

    populatePipelines(executionPipelines, pipelineIds, applicationPipelines, pipelineManager).get
  }

  private def populatePipelines(pipelines: List[Pipeline],
                                pipelineIds: List[String],
                                applicationPipelines: List[Pipeline],
                                pipelineManager: PipelineManager,
                                allowEmpty: Boolean = false): Option[List[Pipeline]] = {
    if (pipelineIds.nonEmpty) {
      // Start with any pipelines that are part of the execution and listed in the pipelineIds
      val filteredExecutionPipelines = pipelines.filter(p => pipelineIds.contains(p.id.getOrElse("")))
      // Get the remaining pipelines listed in pipelineIds
      Some(pipelineIds.filter(id => !filteredExecutionPipelines.exists(_.id.getOrElse("") == id))
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
        }))
    } else if (pipelines.nonEmpty) {
      Some(pipelines)
    } else if (!allowEmpty && applicationPipelines.nonEmpty) {
      Some(applicationPipelines)
    } else {
      if (!allowEmpty) {
        throw new IllegalArgumentException("Either pipelines, pipelineIds or application pipelines must be provided for an execution")
      }
      None
    }
  }

  private def generatePipelineManager(pipelineManagerInfo: Option[ClassInfo],
                                      pipelineManager: Option[PipelineManager],
                                      validateArgumentTypes: Boolean,
                                      credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Option[PipelineManager] = {
    if (pipelineManagerInfo.isDefined && pipelineManagerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineManagerInfo.get.className.getOrElse("com.acxiom.pipeline.CachedPipelineManager"),
        Some(parseParameters(pipelineManagerInfo.get, credentialProvider)), validateArgumentTypes).asInstanceOf[PipelineManager])
    } else {
      pipelineManager
    }
  }

  private def generatePipelineListener(pipelineListenerInfo: Option[ClassInfo],
                                       pipelineListener: Option[PipelineListener],
                                       validateArgumentTypes: Boolean,
                                       credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Option[PipelineListener] = {
    if (pipelineListenerInfo.isDefined && pipelineListenerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(pipelineListenerInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineListener"),
        Some(parseParameters(pipelineListenerInfo.get, credentialProvider)), validateArgumentTypes).asInstanceOf[PipelineListener])
    } else {
      pipelineListener
    }
  }

  private def generateSparkListeners(sparkListenerInfos: Option[List[ClassInfo]],
                                     validateArgumentTypes: Boolean,
                                     credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Option[List[SparkListener]] = {
    if (sparkListenerInfos.isDefined && sparkListenerInfos.get.nonEmpty) {
      Some(sparkListenerInfos.get.flatMap { info =>
        if (info.className.isDefined) {
          Some(ReflectionUtils.loadClass(info.className.get,
            Some(parseParameters(info, credentialProvider)),
            validateArgumentTypes).asInstanceOf[SparkListener])
        } else {
          None
        }
      })
    } else {
      None
    }
  }

  private def generateSecurityManager(securityManagerInfo: Option[ClassInfo],
                                      securityManager: Option[PipelineSecurityManager],
                                      validateArgumentTypes: Boolean,
                                      credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Option[PipelineSecurityManager] = {
    if (securityManagerInfo.isDefined && securityManagerInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(securityManagerInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineSecurityManager"),
        Some(parseParameters(securityManagerInfo.get, credentialProvider)), validateArgumentTypes).asInstanceOf[PipelineSecurityManager])
    } else {
      securityManager
    }
  }

  private def generateStepMapper(stepMapperInfo: Option[ClassInfo],
                                 stepMapper: Option[PipelineStepMapper],
                                 validateArgumentTypes: Boolean,
                                 credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Option[PipelineStepMapper] = {
    if (stepMapperInfo.isDefined && stepMapperInfo.get.className.isDefined) {
      Some(ReflectionUtils.loadClass(stepMapperInfo.get.className.getOrElse("com.acxiom.pipeline.DefaultPipelineStepMapper"),
        Some(parseParameters(stepMapperInfo.get, credentialProvider)), validateArgumentTypes).asInstanceOf[PipelineStepMapper])
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

  private def registerSparkUDFs(globals: Option[Map[String, Any]],
                                sparkSession: SparkSession,
                                sparkUDFs: Option[List[ClassInfo]],
                                validateArgumentTypes: Boolean,
                                credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Unit = {
    if (sparkUDFs.isDefined && sparkUDFs.get.nonEmpty) {
      sparkUDFs.get.flatMap { info =>
        if (info.className.isDefined) {
          Some(ReflectionUtils.loadClass(info.className.get, Some(parseParameters(info, credentialProvider)), validateArgumentTypes).asInstanceOf[PipelineUDF])
        } else {
          None
        }
      }.foreach(udf => udf.register(sparkSession, globals.getOrElse(Map())))
    }
  }

  private def generateGlobals(globals: Option[Map[String, Any]],
                              rootGlobals: Map[String, Any],
                              defaultGlobals: Option[Map[String, Any]],
                              pipelineContext: PipelineContext,
                              merge: Boolean = false)(implicit formats: Formats): Option[Map[String, Any]] = {
    globals.map { baseGlobals =>
      val result = baseGlobals.foldLeft(rootGlobals)((rootMap, entry) => parseValue(rootMap, entry._1, entry._2, Some(pipelineContext)))
      if (merge) {
        defaultGlobals.getOrElse(Map[String, Any]()) ++ result
      } else {
        result
      }
    }.orElse(defaultGlobals)
  }

  private def parseParameters(classInfo: ClassInfo, credentialProvider: Option[CredentialProvider])(implicit formats: Formats): Map[String, Any] = {
    classInfo.parameters.getOrElse(Map[String, Any]())
      .foldLeft(Map[String, Any]("credentialProvider" -> credentialProvider))((rootMap, entry) => parseValue(rootMap, entry._1, entry._2))
  }

  /**
    * This function will parse the provided value and add the result to the provided rootMap using the provided key.
    *
    * @param rootMap The map to store the newly formed object.
    * @param key     The key to use when adding teh result to the rootMap
    * @param value   The value to be parsed
    * @param ctx     The PipelineContext that will provide the mapper
    * @param formats Implicit formats used for JSON conversion
    * @return A map containing the converted value
    */
  def parseValue(rootMap: Map[String, Any], key: String, value: Any, ctx: Option[PipelineContext] = None)(implicit formats: Formats): Map[String, Any] = {
    value match {
      case map: Map[String, Any] if map.contains("className") =>
        val mapEmbedded = map.get("mapEmbeddedVariables").exists(_.toString.toBoolean) && ctx.isDefined
        val finalMap = if (mapEmbedded) {
          ctx.get.parameterMapper.mapEmbeddedVariables(map("object").asInstanceOf[Map[String, Any]], ctx.get)
        } else {
          map("object").asInstanceOf[Map[String, Any]]
        }
        val obj = DriverUtils.parseJson(Serialization.write(finalMap), map("className").asInstanceOf[String])
        rootMap + (key -> obj)
      case listMap: List[Any] =>
        val obj = listMap.map {
          case m: Map[String, Any] =>
            if (m.contains("className")) {
              val mapEmbedded = m.get("mapEmbeddedVariables").exists(_.toString.toBoolean) && ctx.isDefined
              val map = if (m.contains("parameters")) {
                m("parameters").asInstanceOf[Map[String, Any]]
              } else {
                m("object").asInstanceOf[Map[String, Any]]
              }
              val finalMap = if (mapEmbedded) {
                ctx.get.parameterMapper.mapEmbeddedVariables(map, ctx.get)
              } else {
                map
              }
              DriverUtils.parseJson(Serialization.write(finalMap), m("className").asInstanceOf[String])
            } else {
              m
            }
          case any => any
        }
        rootMap + (key -> obj)
      case _ => rootMap + (key -> value)
    }
  }

  private def addSparkListener(pipelineListener: Option[PipelineListener], sparkSession: SparkSession): Unit = {
    if (pipelineListener.isDefined &&
      pipelineListener.get.isInstanceOf[SparkListener]) {
      sparkSession.sparkContext.addSparkListener(pipelineListener.get.asInstanceOf[SparkListener])
    }
    if (pipelineListener.isDefined &&
      pipelineListener.get.isInstanceOf[CombinedPipelineListener] &&
      pipelineListener.get.asInstanceOf[CombinedPipelineListener].listeners.exists(_.isInstanceOf[SparkListener])) {
      pipelineListener.get.asInstanceOf[CombinedPipelineListener].listeners.filter(_.isInstanceOf[SparkListener])
        .foreach(listener => {
          sparkSession.sparkContext.addSparkListener(listener.asInstanceOf[SparkListener])
        })
    }
  }
}
