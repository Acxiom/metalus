package com.acxiom.pipeline.annotations

import scala.annotation.StaticAnnotation

case class StepObject() extends StaticAnnotation

case class StepFunction(id: String,
                        displayName: String,
                        description: String,
                        `type`: String,
                        category: String) extends StaticAnnotation

case class StepParameter(typeOverride: Option[String] = None,
                         required: Option[Boolean] = Some(false),
                         defaultValue: Option[String] = None,
                         language: Option[String] = None) extends StaticAnnotation
