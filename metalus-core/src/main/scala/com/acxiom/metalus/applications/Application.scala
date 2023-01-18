package com.acxiom.metalus.applications

import com.acxiom.metalus.{ClassInfo, Pipeline, PipelineParameter}

case class ApplicationResponse(application: Application)

/**
  * Represents the configuration for a Spark application.
  *
  * @param pipelineId               The main pipeline flow for this application
  * @param stepPackages       List of packages where step objects are located.
  * @param globals            A default set of globals.
  * @param pipelineListener   The pipeline listener class to use while processing.
  * @param stepMapper         An alternate pipeline step mapper class to use while processing.
  * @param pipelineParameters A default set of pipeline parameters to make available while processing.
  * @param requiredParameters A list of parameter names that must be present.
  * @param pipelineManager    An alternate pipeline manager class to use while processing.
  * @param contexts           Map of Context objects to be used for processing.
  */
case class Application(pipelineId: Option[String],
                       pipelineTemplates: List[Pipeline],
                       stepPackages: Option[List[String]],
                       globals: Option[Map[String, Any]],
                       applicationProperties: Option[Map[String, Any]] = None,
                       pipelineListener: Option[ClassInfo] = None,
                       stepMapper: Option[ClassInfo] = None,
                       pipelineParameters: Option[List[PipelineParameter]] = None,
                       requiredParameters: Option[List[String]] = None,
                       pipelineManager: Option[ClassInfo] = None,
                       contexts: Option[Map[String, ClassInfo]])

// TODO Evaluate changing this to just Serializers to support things other than JSON.
case class Json4sSerializers(customSerializers: Option[List[ClassInfo]] = None,
                             enumIdSerializers: Option[List[ClassInfo]] = None,
                             enumNameSerializers: Option[List[ClassInfo]] = None,
                             hintSerializers: Option[List[ClassInfo]] = None)
