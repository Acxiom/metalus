package com.acxiom.metalus.sql

case class ColumnDetails(outputField: String,
                         inputAliases: List[String] = List(),
                         expression: Option[String] = None)
