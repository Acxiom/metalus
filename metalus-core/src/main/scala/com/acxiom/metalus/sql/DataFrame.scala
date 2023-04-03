package com.acxiom.metalus.sql

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.connectors.{Connector, ConnectorProvider, DataConnector, DataStreamOptions, FileConnector}
import com.acxiom.metalus.sql.DataFrame.SHOW_DEFAULT
import com.google.common.collect.ArrayListMultimap
import org.slf4j.{Logger, LoggerFactory}
import tech.tablesaw.aggregate.{AggregateFunction, StringAggregateFunction}
import tech.tablesaw.aggregate.AggregateFunctions._
import tech.tablesaw.api._
import tech.tablesaw.columns.strings.AbstractStringColumn
import tech.tablesaw.selection.Selection
import tech.tablesaw.table.{StandardTableSliceGroup, TableSliceGroup}

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

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
  def where(expression: String): Impl = where(Expression(expression))
  final def filter(expression: Expression): Impl = where(expression)
  final def filter(expression: String): Impl = where(Expression(expression))

  def groupBy(columns: Expression*): GroupedDataFrame[Impl]
  def groupByExpr(columns: String*): GroupedDataFrame[Impl] = groupBy(columns.map(Expression(_)): _*)

  def join(right: Impl, condition: Expression, joinType: String): Impl
  def join(right: Impl, condition: Expression): Impl = join(right, condition, "inner")
  def join(right: Impl, using: Seq[String], joinType: String): Impl
  def join(right: Impl, using: Seq[String]): Impl = join(right, using, "inner")
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

  def connector(conn: Connector): DataFrameWriter

  def save(pipelineContext: PipelineContext): Unit
  def save(path: String, pipelineContext: PipelineContext): Unit
}

final case class ExpressionOptions(copy: Boolean)

trait TableSawQueryable {

  private lazy val tableInternal = table

  private lazy val countInternal = tableInternal.rowCount()

  private lazy val columnLookup: Map[String, Int] = {
    tableInternal.columns().asScala.zipWithIndex.flatMap {
      case (c, i) if c.name.contains("__") =>
        val noAlias = c.name.replaceFirst(".+__", "")
        Map(c.name -> i, noAlias -> i)
      case (c, i) => Map(c.name -> i)
    }.toMap
  }

  protected def table: Table
  protected def alias: Option[String]

  protected def eval(expression: BaseExpression)
                    (implicit options: ExpressionOptions = ExpressionOptions(true)): tech.tablesaw.columns.Column[_] = expression match {
    case i: Identifier => identifier(i)
    case l: Literal => literal(l)
    case Cast(expr, dataType) => cast(eval(expr), dataType)
    case Alias(expr, columnAlias) => eval(expr).setName(columnName(columnAlias.qualifiedName))
    case LogicalNot(expr) => toBooleanColumn(eval(expr)).map((b: java.lang.Boolean) => !b)
    case BinaryOperator(left, right, "AND") => toBooleanColumn(eval(left)) and toBooleanColumn(eval(right))
    case BinaryOperator(left, right, "OR") => toBooleanColumn(eval(left)) or toBooleanColumn(eval(right))
    case BinaryOperator(left, right, "||") => toStringColumn(eval(left)) concatenate eval(right)
    case BinaryOperator(left, right, "+") => toNumericColumn(eval(left)) add toNumericColumn(eval(right))
    case BinaryOperator(left, right, "-") => toNumericColumn(eval(left)) subtract toNumericColumn(eval(right))
    case BinaryOperator(left, right, "*") => toNumericColumn(eval(left)) multiply toNumericColumn(eval(right))
    case BinaryOperator(left, right, "/") => toNumericColumn(eval(left)) divide toNumericColumn(eval(right))
    case BinaryOperator(left, right, "%") =>
      toNumericColumn(eval(left)).remainder(cast(eval(right), "double").asInstanceOf[DoubleColumn])
  }

  //noinspection ScalaStyle
  protected def evalSelection(expression: BaseExpression): Selection = expression match {
    case LogicalNot(expr) => val sel = evalSelection(expr)
      Selection.`with`((0 until countInternal).filterNot(sel.contains): _*)
    case BinaryOperator(left, right, "AND") => evalSelection(left) and evalSelection(right)
    case BinaryOperator(left, right, "OR") => evalSelection(left) or evalSelection(right)
    case BinaryOperator(left, right, "=") => (eval(left), eval(right)) match {
      case (l: AbstractStringColumn[_], r: AbstractStringColumn[_]) => l isEqualTo r
      case (l: NumericColumn[_], r: NumericColumn[_]) => l isEqualTo r
      case (l: BooleanColumn, r) => l isEqualTo toBooleanColumn(r)
      case (l, r: BooleanColumn) => toBooleanColumn(l) isEqualTo r
      case _ => Selection.`with`()
    }
    case BinaryOperator(left, right, "!=") => (eval(left), eval(right)) match {
      case (l: AbstractStringColumn[_], r: AbstractStringColumn[_]) => l isNotEqualTo r
      case (l: NumericColumn[_], r: NumericColumn[_]) => l isNotEqualTo r
      case _ => Selection.`with`(0 until countInternal: _*)
    }
    case BinaryOperator(left, right, ">") => toNumericColumn(eval(left)) isGreaterThan toNumericColumn(eval(right))
    case BinaryOperator(left, right, ">=") => toNumericColumn(eval(left)) isGreaterThanOrEqualTo toNumericColumn(eval(right))
    case BinaryOperator(left, right, "<") => toNumericColumn(eval(left)) isLessThan toNumericColumn(eval(right))
    case BinaryOperator(left, right, "<=") => toNumericColumn(eval(left)) isLessThanOrEqualTo toNumericColumn(eval(right))
    case in: In => evalIn(in)
    case Like(left, right: Literal, not, None) =>
      val regex = right.value.replaceAllLiterally("_", ".").replaceAllLiterally("%", ".*?")
      toStringColumn(eval(left)).matchesRegex(if (not) s"(?!(?:$regex)$$).*" else regex)
    case e => toBooleanColumn(eval(e)).asSelection()
  }

  //noinspection ScalaStyle
  protected def evalIn(in: In): Selection = in match {
    case In(left, not, Some(lits), None) if lits.forall(_.isInstanceOf[Literal]) => eval(left) match {
      case s: AbstractStringColumn[_] if not => s isNotIn lits.map { case l: Literal => l.value }.asJava
      case s: AbstractStringColumn[_] => s isIn lits.map { case l: Literal => l.value }.asJava
      case n: NumericColumn[_] if not =>
        n isNotIn lits.map { case l: Literal => l.value.toDouble.asInstanceOf[java.lang.Number] }.asJava
      case n: NumericColumn[_] =>
        n isIn lits.map { case l: Literal => l.value.toDouble.asInstanceOf[java.lang.Number] }.asJava
      case c if not => toStringColumn(c) isNotIn lits.map { case l: Literal => l.value }.asJava
      case c => toStringColumn(c) isIn lits.map { case l: Literal => l.value }.asJava
    }
    case In(left, not, Some(List(expr)), None) =>
      val l = toStringColumn(eval(left))
      val r = toStringColumn(eval(expr))
      if (not) l isNotIn r else l isIn r
  }

  protected def toNumericColumn(c: tech.tablesaw.columns.Column[_]): NumericColumn[_] = c match {
    case c: NumericColumn[_] => c
    case c => cast(c, "double").asInstanceOf[DoubleColumn]
  }

  protected def toStringColumn(c: tech.tablesaw.columns.Column[_]): AbstractStringColumn[_] = c match {
    case s: StringColumn => s
    case t: TextColumn => t
    case c => c.asStringColumn()
  }

  protected def toBooleanColumn(c: tech.tablesaw.columns.Column[_]): BooleanColumn = c match {
    case b: BooleanColumn => b
    case c => cast(c, "boolean").asInstanceOf[BooleanColumn]
  }

  protected def cast(column: tech.tablesaw.columns.Column[_], dataType: String): tech.tablesaw.columns.Column[_] =
    (column, dataType.toLowerCase) match {
      case (s: StringColumn, _) => castString(s, dataType)
      case (t: TextColumn, _) => castText(t, dataType)
      case (n: NumericColumn[_], "int" | "integer") => n.asIntColumn()
      case (n: NumericColumn[_], "long") => n.asLongColumn()
      case (n: NumericColumn[_], "double") => n.asDoubleColumn()
      case (n: NumericColumn[_], "float") => n.asFloatColumn()
      case (n: NumericColumn[_], "short") => n.asShortColumn()
      case (n: NumericColumn[_], "boolean") => BooleanColumn.create(n.name(), n.asScala.map(_.toString.toDouble == 0)
        .map(_.asInstanceOf[java.lang.Boolean]).toList.asJava)

      //      case (d: DateColumn, "") => d.
      case (c, "string") => c.asStringColumn()
      case (c, "text") => TextColumn.create(c.name, c.asScala.map(_.toString).toList.asJava)
      case (c, unsupported) =>
        throw new IllegalArgumentException(s"Unsupported Cast! Failed to cast ${c.name} to type: [$unsupported]!")
    }

  protected def castString(s: StringColumn, dataType: String): tech.tablesaw.columns.Column[_] = dataType.toLowerCase match {
    case "int" | "integer" => IntColumn.create(s.name, s.asScala.map(stringToInteger).toArray)
    case "long" => stringColToLongCol(s)
    case "double" => DoubleColumn.create(s.name, s.asScala.map(stringToDouble).toList.asJava)
    case "float" => FloatColumn.create(s.name, s.asScala.map(stringToFloat).toArray)
    case "boolean" => BooleanColumn.create(s.name, s.asScala.map(stringToBoolean).toList.asJava)
    case "text" => s.asTextColumn()
    case "string" => s
  }

  private def stringToInteger(s: String): Integer =
    Option(s).filter(_.nonEmpty).map( s => s.toInt.asInstanceOf[Integer]).orNull
  private def stringToDouble(s: String): java.lang.Double =
    Option(s).filter(_.nonEmpty).map(s => s.toDouble.asInstanceOf[java.lang.Double]).orNull
  private def stringToFloat(s: String): java.lang.Float =
    Option(s).filter(_.nonEmpty).map(s => s.toFloat.asInstanceOf[java.lang.Float]).orNull
  private def stringToBoolean(s: String): java.lang.Boolean =
    Option(s).filter(_.nonEmpty).map(s => s.toBoolean.asInstanceOf[java.lang.Boolean]).orNull

  private def stringColToLongCol(s: AbstractStringColumn[_]): LongColumn = {
    val lc = LongColumn.create(s.name)
    s.asScala.foreach {
      case s if !Option(s).exists(_.nonEmpty) => lc.appendMissing()
      case s => lc.append(s.toLong)
    }
    lc
  }

  protected def castText(s: TextColumn, dataType: String): tech.tablesaw.columns.Column[_] = dataType.toLowerCase match {
    case "int" | "integer" => IntColumn.create(s.name, s.asScala.map(stringToInteger).toArray)
    case "long" => stringColToLongCol(s)
    case "double" => DoubleColumn.create(s.name, s.asScala.map(stringToDouble).toList.asJava)
    case "float" => FloatColumn.create(s.name, s.asScala.map(stringToFloat).toArray)
    case "boolean" => BooleanColumn.create(s.name, s.asScala.map(stringToBoolean).toList.asJava)
    case "string" => s.asStringColumn()
    case "text" => s
  }

  protected def literal(lit: Literal): tech.tablesaw.columns.Column[_] = lit match {
    case NullLiteral(_) => StringColumn.create(lit.text, countInternal)
    case IntegerLiteral(i) => LongColumn.create(lit.text, countInternal).setMissingTo(i.value.toLong)
    case DecimalLiteral(d) => DoubleColumn.create(lit.text, countInternal).fillWith(d.value.toDouble)
    case BooleanLiteral(b) => BooleanColumn.create(lit.text, countInternal).setMissingTo(b.value.toLowerCase == "true")
    case Literal(value, _) => StringColumn.create(lit.text, countInternal).setMissingTo(value)
  }

  protected def identifier(ident: Identifier)(implicit options: ExpressionOptions): tech.tablesaw.columns.Column[_] = {
    val col = columnName(ident.qualifiedName)
    val index = columnLookup.get(col)
    if (index.isEmpty) {
      throw new IllegalArgumentException(s"unable to find column: ${ident.qualifiedName}" +
        s" given columns: [${columnLookup.keys.mkString(",")}]")
    }
    val column = table.column(index.get)
    if (options.copy) column.copy() else column
  }

  protected def columnName(identifier: String): String = (identifier, alias) match {
    case (name, Some(tableAlias)) if name.startsWith(s"$tableAlias.") => name.replaceFirst(Pattern.quote("."), "__")
    case (name, Some(tableAlias)) => s"${tableAlias}__$name"
    case (name, None) if name.contains(".") => name.replaceFirst(Pattern.quote("."), "__")
    case (name, _) => name
  }

}

final case class TablesawGroupedDataFrame(dataframe: TablesawDataFrame, groupBy: List[Expression], having: Option[Expression] = None)
  extends GroupedDataFrame[TablesawDataFrame] with TableSawQueryable {
  override def aggregate(columns: Expression*): TablesawDataFrame = {
    val (gb, agg) = getColumns(groupBy.map(_.expressionTree), columns.map(_.expressionTree).toList)
    val groupingColumns = gb.map(eval).map{
      case d: DoubleColumn => d.asLongColumn()
      case c: CategoricalColumn[_] => c
    }
    val temp = Table.create(table.name, groupingColumns: _*)
    val aggregations = agg.map(evalAggregate)
    val lmm = ArrayListMultimap.create[String, AggregateFunction[_,_]]()
    val columnAliases = aggregations.flatMap{ case AggregateColumn(c, agg, columnAlias) =>
      if (!temp.containsColumn(c.name)) temp.addColumns(c)
      lmm.put(c.name, agg)
      columnAlias.map(TableSliceGroup.aggregateColumnName(c.name, agg.functionName) -> _)
    }.toMap
    val grouped = StandardTableSliceGroup.create(temp, groupingColumns: _*)
      .aggregate(lmm)
    grouped.columns()
      .forEach((c: tech.tablesaw.columns.Column[_]) => columnAliases.get(c.name).foreach(c.setName))
    val finalTable = having.map(e => evalSelection(e.expressionTree))
      .map(grouped.where).getOrElse(grouped)
    dataframe.copy(finalTable)
  }

  override def having(expression: Expression): TablesawGroupedDataFrame = copy(having = Some(expression))

  private def getColumns(groupBy: List[BaseExpression], projection: List[BaseExpression]): (List[BaseExpression], List[BaseExpression]) = {
    val (aggregates, other) = projection.partition{
      case Alias(expr, _) => aggregateFunctions.isDefinedAt(expr)
      case e => aggregateFunctions.isDefinedAt(e)
    }
    (other.collect{
      case a@Alias(expr, _) if groupBy.contains(expr) => a
      case e if groupBy.contains(e) => e
    }, aggregates)
  }

  private val aggregateFunctions: PartialFunction[BaseExpression, AggregateColumn] = {
    implicit val options: ExpressionOptions = ExpressionOptions(false)
    val res: PartialFunction[BaseExpression, AggregateColumn] = {
      case Alias(expr, alias) => evalAggregate(expr).copy(alias = Some(alias.value))
      case SqlFunction("COUNT", Some(List(parameter))) => AggregateColumn(eval(parameter), count, None)
      case SqlFunction("MIN", Some(List(parameter))) => AggregateColumn(eval(parameter), min, None)
      case SqlFunction("MAX", Some(List(parameter))) => AggregateColumn(eval(parameter), max, None)
      case SqlFunction("AVG" | "AVERAGE" | "MEAN", Some(List(parameter))) => AggregateColumn(eval(parameter), mean, None)
      case SqlFunction("MEDIAN", Some(List(parameter))) => AggregateColumn(eval(parameter), median, None)
      case SqlFunction("SUM", Some(List(parameter))) => AggregateColumn(eval(parameter), sum, None)
      case SqlFunction("PRODUCT", Some(List(parameter))) => AggregateColumn(eval(parameter), product, None)
      case SqlFunction("CONCAT", Some(List(parameter))) =>
        AggregateColumn(eval(parameter), new ConcatStringAggregate(",", false), None)
      case SqlFunction("CONCAT", Some(List(parameter, Literal(sep, _)))) =>
        AggregateColumn(eval(parameter), new ConcatStringAggregate(sep, false), None)
      case SqlFunction("CONCAT_DISTINCT", Some(List(parameter))) =>
        AggregateColumn(eval(parameter), new ConcatStringAggregate(",", true), None)
      case SqlFunction("CONCAT_DISTINCT", Some(List(parameter, Literal(sep, _)))) =>
        AggregateColumn(eval(parameter), new ConcatStringAggregate(sep, true), None)
    }
    res
  }

  private def evalAggregate(expression: BaseExpression): AggregateColumn = aggregateFunctions(expression)

  override protected def table: Table = dataframe.table
  override protected def alias: Option[String] = dataframe.alias
}

final class ConcatStringAggregate(sep: String, distinct: Boolean)
  extends StringAggregateFunction(s"concat${if(distinct)"_distinct" else ""}") {
  override def summarize(column: StringColumn): String = {
    val array = column.asObjectArray()
    if (distinct) array.toSet.mkString(sep) else array.mkString(sep)
  }
}

private final case class AggregateColumn(column: tech.tablesaw.columns.Column[_], func: AggregateFunction[_, _], alias: Option[String])

final case class TablesawDataFrame(table: Table, alias: Option[String] = None) extends DataFrame[TablesawDataFrame] with TableSawQueryable {


  private lazy val schemaInternal = Schema(table.columnArray().map { c =>
    val name = c.name.replaceFirst(".+__", "")
    Attribute(name, AttributeType(c.`type`().name), Some(true), None)
  }.toList)
  override def schema: Schema = schemaInternal

  override def count(): Long = table.rowCount()

  override def collect(): Array[Row] = table.asScala.map { r =>
    Row((0 until r.columnCount()).map(r.getObject).toArray, Some(schema), None)
  }.toArray

  override def select(columns: Expression*): TablesawDataFrame = {
    val newColumns = columns.map(e => eval(e.expressionTree))
    copy(table.selectColumns(newColumns: _*))
  }

  override def where(expression: Expression): TablesawDataFrame = copy(table.where(evalSelection(expression.expressionTree)))

  override def groupBy(columns: Expression*): TablesawGroupedDataFrame = TablesawGroupedDataFrame(this, columns.toList)

  override def join(right: TablesawDataFrame, condition: Expression, joinType: String): TablesawDataFrame = {
    throw new UnsupportedOperationException("Conditional joins are not supported at this time.")
  }

  override def join(right: TablesawDataFrame, using: Seq[String], joinType: String): TablesawDataFrame = {
    val joiner = table.joinOn(using.map(columnName): _*)
    val joinTable = joinType.toLowerCase match {
      case "inner" => joiner.inner(right.table, true, false)
      case "left"|"left_outer"|"left outer" => joiner.leftOuter(right.table, true, false)
      case "right"|"right_outer"|"right outer" => joiner.rightOuter(right.table, true, false)
      case "full"|"full_outer"|"full outer" => joiner.fullOuter(right.table, true, false)
      case unsupported => throw new IllegalArgumentException(s"Join type: $unsupported is not supported.")
    }
    copy(joinTable, None)
  }

  override def crossJoin(right: TablesawDataFrame): TablesawDataFrame = {
    val rightTable = right.table
    val rightSize = rightTable.rowCount()
    val newTable = table.emptyCopy()
      .addColumns(rightTable.columns().asScala.map(r => r.emptyCopy()): _*)
    val cols: Map[String, tech.tablesaw.columns.Column[_]] = newTable.columnArray().map(c => c.name -> c).toMap
    val leftCols = table.columnArray()
    val rightCols = rightTable.columnArray()
    (0 until table.rowCount()).foreach{ i =>
      (0 until rightSize).foreach{ j =>
        leftCols.foreach(c => cols(c.name).appendObj(c.get(i)))
        rightCols.foreach(c => cols(c.name).appendObj(c.get(j)))
      }
    }
    copy(newTable, alias = None)
  }

  override def union(other: TablesawDataFrame): TablesawDataFrame = copy(table.append(other.table))

  override def distinct(): TablesawDataFrame = copy(table.dropDuplicateRows())

  override def orderBy(expression: Expression): TablesawDataFrame = this

  override def limit(limit: Int): TablesawDataFrame = copy(table.first(limit))

  //noinspection ScalaStyle
  override def show(rows: Int): Unit = println(table.print(rows))

  override def as(alias: String): TablesawDataFrame = {
    val newTable = table.copy()
    newTable.columnArray().foreach(c => c.setName(s"${alias}__${c.name}"))
    copy(table = newTable, alias = Some(alias))
  }

  override def write: DataFrameWriter = TablesawDataFrameWriter()

  final case class TablesawDataFrameWriter(format: Option[String] = None,
                                     options: Option[Map[String, String]] = None,
                                     connector: Option[Connector] = None,
                                    ) extends DataFrameWriter {
    private lazy val internalFormat = format.getOrElse("csv")

    override def format(dataFormat: String): DataFrameWriter = copy(format = Some(dataFormat))

    override def option(key: String, value: String): DataFrameWriter =
      copy(options = Some(options.getOrElse(Map()) + (key -> value)))

    override def options(opts: Map[String, String]): DataFrameWriter =
      copy(options = Some(options.getOrElse(Map()) ++ opts))

    override def connector(conn: Connector): DataFrameWriter = copy(connector = Some(conn))

    override def save(pipelineContext: PipelineContext): Unit = {
      val uri = options.flatMap(o => o.get("uri").orElse(o.get("path"))).getOrElse(internalFormat)
      save(uri, pipelineContext)
    }

    override def save(uri: String, pipelineContext: PipelineContext): Unit = {
      val conn = connector.orElse(ConnectorProvider.getConnector(s"${table.name}_write", uri, None, options))
      if (conn.isEmpty) {
        throw new IllegalArgumentException(s"Unable to get Connector for uri: [$uri]")
      }
      conn.get match {
        case fc: FileConnector =>
          val os = fc.getFileManager(pipelineContext).getFileResource(uri).getOutputStream()
          table.write().toStream(os, internalFormat)
          os.close()
        case dc: DataConnector =>
          val writer = dc.getWriter(Some(DataStreamOptions(Some(schema), options.get)), pipelineContext)
          if (writer.isEmpty) {
            throw new IllegalArgumentException(s"Unable to get DataRowWriter from connector: [$dc]")
          }
          writer.get.process(collect().toList)
      }
    }
  }

}




