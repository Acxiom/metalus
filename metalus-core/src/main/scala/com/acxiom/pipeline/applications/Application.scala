package com.acxiom.pipeline.applications

import com.acxiom.pipeline.{DefaultPipeline, PipelineParameters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

case class ApplicationResponse(application: Application)

/**
  * Represents the configuration for a Spark application.
  *
  * @param executions         List of executions that will be used to generate the Execution Plan.
  * @param stepPackages       List of packages where step objects are located.
  * @param globals            A default set of globals.
  * @param pipelines          A list of pipelines that can be used by the executions.
  * @param pipelineListener   The pipeline listener class to use while processing.
  * @param securityManager    An alternate security manager class to use while processing.
  * @param stepMapper         An alternate pipeline step mapper class to use while processing.
  * @param pipelineParameters A default set of pipeline parameters to make available while processing.
  * @param sparkConf          Configuration items to set on the SparkConf when it is created.
  * @param requiredParameters A list of parameter names that must be present.
  * @param pipelineManager    An alternate pipeline manager class to use while processing.
  * @param sparkUdfs          List of UDF classes to register.
  * @param json4sSerializers  Object containing classes for custom json4s serializers.
  */
case class Application(executions: Option[List[Execution]],
                       stepPackages: Option[List[String]],
                       globals: Option[Map[String, Any]],
                       applicationProperties: Option[Map[String, Any]] = None,
                       pipelines: Option[List[DefaultPipeline]] = None,
                       pipelineListener: Option[ClassInfo] = None,
                       sparkListeners: Option[List[ClassInfo]] = None,
                       securityManager: Option[ClassInfo] = None,
                       stepMapper: Option[ClassInfo] = None,
                       pipelineParameters: Option[PipelineParameters] = None,
                       sparkConf: Option[Map[String, Any]] = None,
                       requiredParameters: Option[List[String]] = None,
                       pipelineManager: Option[ClassInfo] = None,
                       sparkUdfs: Option[List[ClassInfo]] = None,
                       json4sSerializers: Option[Json4sSerializers] = None)

/**
  * Represents a single execution of a Spark application.
  *
  * @param id                    The unique id of this execution to be used by dependent executions.
  * @param pipelines             A list of pipelines to execute.
  * @param pipelineIds           A List of pipelineIds, referencing the pipelines defined in the application.
  * @param initialPipelineId     The id of the first pipeline that should be executed.
  * @param mergeGlobals          Defines whether to merge or overwrite the application globals with the local globals.
  * @param globals               A default set of globals for this execution.
  * @param parents               A list of parent execution ids.
  * @param pipelineListener      The pipeline listener class to use while processing this execution.
  * @param securityManager       An alternate security manager class to use while processing this execution.
  * @param stepMapper            An alternate pipeline step mapper class to use while processing this execution.
  * @param pipelineParameters    A default set of pipeline parameters to make available while processing this execution.
  * @param evaluationPipelines   A list of pipelines to execute when evaluating the run status of this execution.
  * @param evaluationPipelineIds A list of pipeline ids to execute when evaluating the run status of this execution.
  * @param forkByValue           A global path to an array of values to use for forking this execution.
  * @param executionType         An optional type parameter that may be pipeline (default), fork or join
  */
case class Execution(id: Option[String],
                     pipelines: Option[List[DefaultPipeline]] = None,
                     pipelineIds: Option[List[String]] = None,
                     initialPipelineId: Option[String] = None,
                     mergeGlobals: Option[Boolean] = Some(false),
                     globals: Option[Map[String, Any]] = None,
                     parents: Option[List[String]] = None,
                     pipelineListener: Option[ClassInfo] = None,
                     sparkListeners: Option[List[ClassInfo]] = None,
                     securityManager: Option[ClassInfo] = None,
                     stepMapper: Option[ClassInfo] = None,
                     pipelineParameters: Option[PipelineParameters] = None,
                     evaluationPipelines: Option[List[DefaultPipeline]] = None,
                     evaluationPipelineIds: Option[List[String]] = None,
                     forkByValue: Option[String] = None,
                     executionType: Option[String] = None)

/**
  * Contains information about a class that needs to be instantiated at runtime.
  *
  * @param className The fully qualified class name.
  * @param parameters A map of simple parameters to pass to the constructor.
  */
case class ClassInfo(className: Option[String], parameters: Option[Map[String, Any]] = None)

/**
 * Trait that can be extended to register spark udfs.
 */
trait PipelineUDF extends Serializable {

  /**
   * This method should be used to register a udf with the sparkSession object passed to it.
   * @param sparkSession The spark session.
   * @param globals      Application level globals.
   * @return             A spark UserDefinedFunction object.
   */
  def register(sparkSession: SparkSession, globals: Map[String, Any]): UserDefinedFunction
}

case class ApplicationTriggers(enableHiveSupport: Boolean = false,
                               parquetDictionaryEnabled: Boolean = true,
                               validateArgumentTypes: Boolean = false)

case class Json4sSerializers(customSerializers: Option[List[ClassInfo]] = None,
                             enumIdSerializers: Option[List[ClassInfo]] = None,
                             enumNameSerializers: Option[List[ClassInfo]] = None,
                             hintSerializers: Option[List[ClassInfo]] = None)
