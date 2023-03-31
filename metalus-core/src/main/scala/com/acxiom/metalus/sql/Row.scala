package com.acxiom.metalus.sql

/**
 * Represents a simple row of data. An optional schema may be provided to provide structure.
 *
 * @param columns   The positional data for the row.
 * @param schema    An optional schema that provides metadata about each column
 * @param nativeRow The original data representation.
 */
final case class Row(columns: Array[_], schema: Option[Schema], nativeRow: Option[Any]) {
  private lazy val indexLookup = schema.map(_.attributes.zipWithIndex.map{ case (a, i) => a.name -> i}.toMap)
    .getOrElse(Map())
  def mkString(sep: String): String = columns.mkString(sep)

  def apply(index: Int): Any = columns(index)
  def apply(column: String): Any = {
    val value = get(column)
    if (value.isEmpty){
      throw new IllegalArgumentException(s"Unable to find column: [$column] among fields: ${indexLookup.keys.mkString(", ")}")
    }
    value.get
  }

  def get(index: Int): Option[Any] = if (index < columns.length) Some(columns(index)) else None
  def get(column: String): Option[Any] = indexLookup.get(column).map(columns.apply)
}
