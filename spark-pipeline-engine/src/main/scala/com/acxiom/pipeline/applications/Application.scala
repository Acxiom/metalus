package com.acxiom.pipeline.applications

import com.acxiom.pipeline.{DefaultPipeline, PipelineParameters}

case class Application(executions: Option[List[Execution]],
                       stepPackages: Option[List[String]],
                       globals: Option[Map[String, Any]],
                       pipelineListener: Option[ClassInfo] = None,
                       securityManager: Option[ClassInfo] = None,
                       stepMapper: Option[ClassInfo] = None,
                       pipelineParameters: Option[PipelineParameters] = None,
                       sparkConf: Option[Map[String, Any]] = None)

case class Execution(id: Option[String],
                     pipelines: Option[List[DefaultPipeline]],
                     initialPipelineId: Option[String] = None,
                     globals: Option[Map[String, Any]] = None,
                     parents: Option[List[String]] = None,
                     pipelineListener: Option[ClassInfo] = None,
                     securityManager: Option[ClassInfo] = None,
                     stepMapper: Option[ClassInfo] = None,
                     pipelineParameters: Option[PipelineParameters] = None)

case class ClassInfo(className: Option[String], parameters: Option[Map[String, Any]] = None)
