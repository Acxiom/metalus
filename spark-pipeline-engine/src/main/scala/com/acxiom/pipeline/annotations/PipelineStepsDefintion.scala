package com.acxiom.pipeline.annotations

import com.acxiom.pipeline.EngineMeta

case class PipelineStepsDefintion(pkgs: List[String],
                                  steps: List[StepDefinition])

case class StepDefinition(id: String,
                          displayName: String,
                          description: String,
                          `type`: String,
                          parameters: List[StepFunctionParameter],
                          engineMeta: EngineMeta)

case class StepFunctionParameter(`type`: String, name: String)
