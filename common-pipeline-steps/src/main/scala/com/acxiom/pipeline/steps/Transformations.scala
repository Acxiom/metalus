package com.acxiom.pipeline.steps


case class ColumnDetails(outputField: String,
                         inputAliases: List[String] = List(),
                         expression: Option[String] = None)

case class Transformations(columnDetails: List[ColumnDetails],
                           filter: Option[String] = None,
                           standardizeColumnNames: Option[Boolean] = Some(false))
