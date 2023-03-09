package com.acxiom.metalus.sql

/**
 * Represents a simple row of data. An optional schema may be provided to provide structure.
 *
 * @param columns   The positional data for the row.
 * @param schema    An optional schema that provides metadata about each column
 * @param nativeRow The original data representation.
 */
case class Row(columns: Array[Any], schema: Option[Schema], nativeRow: Option[Any]) {
  def mkString(sep: String): String = columns.mkString(sep)
}
