package com.acxiom.metalus.steps

import com.acxiom.pipeline.EngineMeta

case class PipelineStepsDefinition(pkgs: List[String],
                                   steps: List[StepDefinition],
                                   pkgObjs: List[PackageObject])

case class StepDefinition(id: String,
                          displayName: String,
                          description: String,
                          `type`: String,
                          category: String,
                          params: List[StepFunctionParameter],
                          engineMeta: EngineMeta,
                          tags: List[String])

case class StepFunctionParameter(`type`: String,
                                 name: String,
                                 required: Boolean = false,
                                 defaultValue: Option[String] = None,
                                 language: Option[String] = None,
                                 className: Option[String] = None,
                                 parameterType: Option[String] = None,
                                 description: Option[String] = None)

case class PackageObject(id: String, schema: String)
