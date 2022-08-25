package com.acxiom.pipeline.annotations

import scala.annotation.StaticAnnotation

case class StepObject() extends StaticAnnotation

case class StepFunction(id: String,
                        displayName: String,
                        description: String,
                        `type`: String,
                        category: String,
                        tags: List[String] = List()) extends StaticAnnotation

case class StepParameter(typeOverride: Option[String] = None,
                         required: Option[Boolean] = Some(false),
                         defaultValue: Option[String] = None,
                         language: Option[String] = None,
                         className: Option[String] = None,
                         parameterType: Option[String] = None,
                         description: Option[String] = None) extends StaticAnnotation

case class StepParameters(parameters: Map[String, StepParameter]) extends StaticAnnotation

case class PrivateObject() extends StaticAnnotation

case class BranchResults(names: List[String]) extends StaticAnnotation

case class StepResults(primaryType: String, secondaryTypes: Option[Map[String, String]]) extends StaticAnnotation
