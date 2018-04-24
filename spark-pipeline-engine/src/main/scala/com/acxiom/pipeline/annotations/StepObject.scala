package com.acxiom.pipeline.annotations

import scala.annotation.StaticAnnotation

case class StepObject() extends StaticAnnotation

case class StepFunction(id: String,
                        displayName: String,
                        description: String,
                        `type`: String) extends StaticAnnotation
