package com.acxiom.metalus.sql

case class Transformations(columnDetails: List[ColumnDetails],
                           filter: Option[String] = None,
                           standardizeColumnNames: Option[Boolean] = Some(false))
