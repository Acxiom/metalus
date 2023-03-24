package com.acxiom.metalus.sql

import com.acxiom.metalus.sql.DataFrame.SHOW_DEFAULT

object DataFrame {
  val SHOW_DEFAULT = 20
}

trait DataFrame[Impl <: DataFrame[Impl]] {

  def schema: Schema

  def collect(): Array[Row]
  def take(n: Int): Array[Row] = collect().take(n)
  def toList: List[Row] = collect().toList

  def select(columns: Expression*): Impl
  def selectExpr(expressions: String*): Impl = select(expressions.map(e => Expression(e)): _*)

  def where(expression: Expression): Impl
  final def filter(expression: Expression): Impl = where(expression)

  def groupBy(columns: Expression*): GroupedDataFrame[Impl]

  def join(right: Impl, condition: Expression, joinType: String): Impl
  def join(right: Impl, condition: Expression): Impl = join(right, condition, "inner")
  def join(right: Impl, using: Seq[Expression], joinType: String): Impl
  def join(right: Impl, using: Seq[Expression]): Impl = join(right, using, "inner")
  def crossJoin(right: Impl): Impl

  def union(other: Impl): Impl

  def distinct(): Impl

  def count(): Long = collect().length

  def orderBy(expression: Expression): Impl
  def sortBy(expression: Expression): Impl = orderBy(expression)

  def limit(limit: Int): Impl

  def show(rows: Int = SHOW_DEFAULT): Unit

  def as(alias: String): Impl

  def write: DataFrameWriter

}

trait GroupedDataFrame[Impl <: DataFrame[Impl]] {
  def aggregate(columns: Expression*): Impl
  def aggregateExpr(expressions: String*): Impl = aggregate(expressions.map(e => Expression(e)): _*)

  def having(expression: Expression): GroupedDataFrame[Impl]
}

trait DataFrameWriter {
  def format(dataFormat: String): DataFrameWriter
  def option(key: String, value: String): DataFrameWriter
  def options(opts: Map[String, String]): DataFrameWriter
  def save(): Unit
  def save(path: String): Unit
}

//case class TablesawDataFrame(table: Table) extends DataFrame {
//
//  override def schema: Schema =
//    Schema(table.columnArray().map(c => Attribute(c.name, AttributeType(c.`type`().name), Some(true), None)).toList)
//
//  private def convertRow(row: tech.tablesaw.api.Row): Row = None.orNull
//}


