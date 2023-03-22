package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.{ConnectorProvider, FileConnector}
import com.acxiom.metalus.{PipelineContext, PipelineException}
import com.acxiom.metalus.utils.ReflectionUtils
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, UnescapedQuoteHandling}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters._
import scala.math.ScalaNumber



object InMemoryTable {
  def fromMap(data: List[Map[String, Any]], cleanData: Boolean = true): InMemoryTable = {
    val schema = Schema(data.map(r => rowToAttributes(r)).reduce(coerceAttributes).values.toList)
    val cleanedData = if (cleanData){
      val withIndex = schema.attributes.zipWithIndex
      data.map{ row =>
        val newRow = Array.ofDim[Any](withIndex.size)
        withIndex.foreach{ case (a, i) => newRow(i) = row(a.name)}
        newRow
      }
    } else {
      data.map(_.values.toArray)
    }
    InMemoryTable(cleanedData, schema)
  }

  def fromSeq(data: Seq[Array[Any]], schema: Option[Schema], cleanData: Boolean = true): InMemoryTable = {
    val finalSchema = schema.getOrElse{
      Schema(data.map(row => rowToAttributes(row.zipWithIndex.map{case (a, i) => s"col_$i" -> a}.toMap))
        .reduce(coerceAttributes).values.toList)
    }
    val cleanedData = if(cleanData) {
      data.map{ row =>
        val newRow = Array.ofDim[Any](finalSchema.attributes.size)
        row.zipWithIndex.foreach{ case (v, i) => newRow(i) = v}
        newRow
      }.toList
    } else {
      data.toList
    }
    InMemoryTable(cleanedData, finalSchema)
  }

  def fromCsv(data: InputStream, schema: Option[Schema], options: Option[Map[String, Any]]): InMemoryTable = {
    val params = options.map(_.mapValues(_.toString).toMap).getOrElse(Map.empty[String, String])
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setComment('\0')
    format.setDelimiter(params.getOrElse("delimiter", params.getOrElse("separator", ",")))
    params.get("quote").foreach(q => format.setQuote(q.head))
    params.get("escape").foreach(e => format.setQuoteEscape(e.head))
    params.get("recordDelimiter").foreach(r => format.setLineSeparator(r))
    settings.setEmptyValue("")
    settings.setNullValue("")
    settings.setHeaderExtractionEnabled(params.get("header").map( _.toLowerCase == "true").getOrElse(schema.isEmpty))
    schema.foreach(s => settings.setHeaders(s.attributes.map(_.name): _*))
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_CLOSING_QUOTE)
    val csvParser = new CsvParser(settings)

    val rows = csvParser.parseAllRecords(data, params.getOrElse("encoding", "utf-8")).asScala.toList
    val finalSchema = schema.getOrElse{
      val inferTypes = params.get("inferDataTypes").exists(_.toBoolean)
      if (inferTypes) {
        Schema(rows.map { record =>
          rowToAttributes(record.getMetaData.headers().map(h => h -> record.getString(h)).toMap, inferString = true)
        }.reduce(coerceAttributes).values.toList)
      } else {
        Schema(csvParser.getRecordMetadata.headers().map(name => Attribute(name, AttributeType("String"), Some(true), None)))
      }
    }
    val finalData = rows.map{ record =>
      finalSchema.attributes.map{ field =>
        field.dataType.baseType.toLowerCase match {
          case "int" | "bigint" | "long" | "short" | "byte" => BigInt(record.getBigInteger(field.name))
          case "float" | "double" | "decimal" | "bigdecimal" => BigDecimal(record.getBigDecimal(field.name))
          case "boolean" => record.getBoolean(field.name)
          case _ => record.getString(field.name)
        }
      }.toArray[Any]
    }
    InMemoryTable(finalData, finalSchema)
  }

  private def rowToAttributes(row: Map[_, _], inferString: Boolean = false): Map[String, Attribute] = row.map {
    case (k: String, v) => k -> Attribute(k, getType(v, inferString), Some(true), None)
    case (k, v) => k.toString -> Attribute(k.toString, getType(v, inferString), Some(true), None)
  }

  private def getType(value: Any, inferString: Boolean = false): AttributeType = value match {
    case _: Boolean => AttributeType("Boolean")
    case v: String if inferString => inferStringValue(v)
    case _: String | _: Char => AttributeType("String")
    case _: Byte | _: Short  | _: Int | _: Long | _: BigInt => AttributeType("BigInt")
    case _: Float | _: Double | _: BigDecimal => AttributeType("Decimal")
    case a: Array[_] =>
      AttributeType("Array", a.headOption.map(e => getType(e, inferString)).orElse(Some(AttributeType("Binary"))))
    case m: Map[_, _] => AttributeType("Struct", schema = Some(Schema(rowToAttributes(m).values.toList)))
    case n if n == None.orNull => AttributeType("Null")
  }

  private def coerceAttributes(left: Map[String, Attribute], right: Map[String, Attribute]): Map[String, Attribute] = {
    right.foldLeft(left) { case (attrs, p@(key, attr)) =>
        attrs.get(key).map {
          case leftAttr if leftAttr == attr => attrs
          case _ if attr.dataType.baseType == "Null" => attrs
          case _ => attrs + (key -> attr.copy(dataType = AttributeType("Binary")))
        } getOrElse {
          attrs + p
        }
    }
  }

  private def inferStringValue(value: String): AttributeType = value match {
    case i if i.forall(_.isDigit) => AttributeType("BigInt")
    case d if d.indexOf('.') == d.lastIndexOf('.') && d.forall(c => c == '.' || c.isDigit) => AttributeType("Decimal")
    case b if b.toLowerCase == "true" || b.toLowerCase == "false" => AttributeType("Boolean")
    case _ => AttributeType("String")
  }
}

final case class InMemoryTable(data: List[Array[Any]], schema: Schema) {
  def toMap: List[Map[String,Any]] = {
    val keys = schema.attributes.map(_.name)
    data.map(row => keys.zip(row).toMap)
  }
}


final case class InMemoryDataReference(base: () => InMemoryTable,
                      origin: DataReferenceOrigin,
                      pipelineContext: PipelineContext,
                      logicalPlan: Queue[QueryOperator] = Queue(),
                      tableAlias: Option[String] = None)
  extends LogicalPlanDataReference[InMemoryTable, InMemoryTable] with ConvertableReference {

  type Row = Array[Any]

  private def getIndexMap(tableSchema: Schema): Map[String, Int] = tableSchema.attributes.zipWithIndex.flatMap {
    case (a, i) if tableAlias.nonEmpty => Seq(a.name -> i, s"${tableAlias.mkString}.${a.name}" -> i)
    case (a, i) => Seq(a.name -> i)
  }.toMap

  private lazy val internalReference = base()

  override def initialReference: InMemoryTable = internalReference

  override def engine: String = "memory"

  override def execute: InMemoryTable = executePlan

  override protected def logicalPlanRules: LogicalPlanRules = {
    case Select(expressions) => evalProjection(expressions, _)
    case Where(expression) => table =>
      val indexMap = getIndexMap(table.schema)
      table.copy(data = table.data.filter(row => evalBoolean(expression.expressionTree, indexMap.get(_).map(row.apply))))
    case GroupBy(expressions) => evalGroupBy(expressions, _)
    case Join(right: InMemoryDataReference, "INNER", Some(condition), _) => left =>
      val rightTable = right.execute
      val leftIndices = getIndexMap(left.schema)
      val rightIndices = right.getIndexMap(rightTable.schema)
      val getVal: (String, Row, Row) => Option[Any] = { (key, leftRow, rightRow) =>
        leftIndices.get(key).map(leftRow.apply).orElse(rightIndices.get(key).map(rightRow.apply))
      }
      val joinedData = left.data.flatMap{ leftRow =>
        rightTable.data.collect{
          case rightRow if evalBoolean(condition.expressionTree, getVal(_, leftRow, rightRow)) => leftRow ++ rightRow
        }
      }
      InMemoryTable(joinedData, Schema(left.schema.attributes ++ rightTable.schema.attributes))
    case Union(right: InMemoryDataReference, false) => left => {
      val attributes = left.schema.attributes
      val leftIndices = getIndexMap(left.schema)
      val rightTable = right.execute
      val rightIndices = right.getIndexMap(rightTable.schema)
      left.copy(data = left.data ++ right.execute.data.map { row =>
        val newRow = Array.ofDim[Any](attributes.size)
        attributes.foreach(a => newRow(leftIndices(a.name)) = row(rightIndices(a.name)))
        newRow
      })
    }
    case Limit(limit) => table => table.copy(data = table.data.take(limit))
  }

  override protected def queryOperations: QueryFunction = {
    case As(alias) => copy(tableAlias = Some(alias))
    case qo if logicalPlanRules.isDefinedAt(qo) => copy(logicalPlan = updateLogicalPlan(qo))
  }

  def toBoolean(value: Option[Any]): Boolean = value.exists {
    case b: Boolean => b
    case s: String => s.toLowerCase != "false"
    case _ => true
  }

  private def toNumeric(value: Option[Any]): Option[ScalaNumber] = value.map {
    case bi: BigInt => bi
    case bd: BigDecimal => bd
    case v =>
      val s = v.toString
      if (s.contains(".")) BigDecimal(s) else BigInt(s)
  }

  def evalGroupBy(expressions: List[Expression], table: InMemoryTable): InMemoryTable = {
    val indexMap = getIndexMap(table.schema)
    val groupAttributes = expressions.map(e => getAttribute(e.expressionTree, e.text, indexMap))
    val listAttributes = table.schema.attributes.map { attr =>
      attr.copy(dataType = AttributeType("Array", Some(attr.dataType)))
    }.filter(a => !groupAttributes.exists(_.name == a.name))
    val newSchema = Schema(groupAttributes ++ listAttributes)
    val newData = table.data.groupBy{ row =>
      expressions.map(e => eval(e.expressionTree, indexMap.get(_).map(row.apply)).orNull)
    }.map{ case (key, list) =>
      val array = Array.ofDim[Any](listAttributes.size, list.size)
      list.zipWithIndex.foreach{ case (row, j) =>
        listAttributes.zipWithIndex.foreach{ case (a, i) =>
          array(i)(j) = row(indexMap(a.name))
        }
      }
      key.toArray ++ array
    }.toList
    InMemoryTable(newData, newSchema)
  }

  def evalProjection(expressions: List[Expression], table: InMemoryTable): InMemoryTable = {
    val indexMap = getIndexMap(table.schema)
    val schema = Schema(expressions.map(e => getAttribute(e.expressionTree, e.text, indexMap)))
    val data = table.data.map{ row =>
      expressions.map(e => eval(e.expressionTree, indexMap.get(_).map(row.apply)).orNull).toArray
    }
    InMemoryTable(data, schema)
  }

  def evalBoolean(expr: BaseExpression, row: String => Option[Any]): Boolean = toBoolean(eval(expr, row))

  //noinspection ScalaStyle
  def getAttribute(expr: BaseExpression, defaultName: String, indices: Map[String, Int]): Attribute = expr match {
    case Alias(expr, alias) => getAttribute(expr, defaultName, indices).copy(name = alias.value)
    case l: Literal => literalAttribute(l)
    case i: Identifier => identifierAttribute(i, indices)
    case Cast(expr, dataType) => castAttribute(getAttribute(expr, defaultName, indices), dataType)
    case l: LogicalNot => Attribute("", AttributeType("Boolean"), Some(false), None)
    case BinaryOperator(_, _, "AND"|"OR"|"XOR"|"="|"!=") => Attribute("", AttributeType("Boolean"), Some(false), None)
    case BinaryOperator(left, right, "||") =>
      val leftAttr = getAttribute(left, defaultName, indices)
      val rightAttr = getAttribute(right, defaultName, indices)
      if (leftAttr.dataType != rightAttr.dataType) Attribute("", AttributeType("Binary"), Some(false), None) else leftAttr
    case UnaryOperator(_, "~") => Attribute("", AttributeType("BigInt"), Some(true), None)
    case UnaryOperator(expr, "-") => getAttribute(expr, defaultName, indices) match {
      case a if a.dataType.baseType.toUpperCase == "BIGINT" || a.dataType.baseType.toUpperCase == "DECIMAL" => a
      case a => a.copy(dataType = AttributeType("Binary"))
    }
    case SqlFunction("COUNT", Some(List(_))) => Attribute(defaultName, AttributeType("BigInt"), Some(false), None)
  }

  //noinspection ScalaStyle
  def eval(expr: BaseExpression, row: String => Option[Any]): Option[Any] = expr match {
    case l: Literal => literal(l)
    case i: Identifier => identifier(i, row)
    case Alias(expr, _) => eval(expr, row)
    case c: Cast => cast(eval(c.expr, row), c.dataType)
    case UnaryOperator(expr, op@("~" | "-")) => toNumeric(eval(expr, row)).map {
      case bd: BigDecimal => if (op == "~") ~bd.toBigInt else -bd
      case bi: BigInt => if (op == "~") ~bi else -bi
    }
    case LogicalNot(expr) => Some(!toBoolean(eval(expr, row)))
    case BinaryOperator(left, right, "AND") => Some(toBoolean(eval(left, row)) && toBoolean(eval(right, row)))
    case BinaryOperator(left, right, "OR") => Some(toBoolean(eval(left, row)) || toBoolean(eval(right, row)))
    case BinaryOperator(left, right, "XOR") => Some(toBoolean(eval(left, row)) ^ toBoolean(eval(right, row)))
    case BinaryOperator(left, right, "||") => eval(left, row) orElse eval(right, row)
    case BinaryOperator(left, right, "=") => Some(eval(left, row) == eval(right, row))
    case BinaryOperator(left, right, "!=") => Some(eval(left, row) != eval(right, row))
    // aggregations
    case SqlFunction("COUNT", Some(List(parameter))) => eval(parameter, row).map {
      case l: List[_] => l.size
      case a: Array[_] => a.length
    }
    //    case SqlFunction("SUM", Some(List(parameter))) => eval(parameter, row).map {
    //      case l: List[_] => l.flatMap(v => toNumeric(Option(v))).sum
    //    }
    case _ => None
  }

  def cast(value: Option[Any], dataType: String): Option[Any] = value.map { v =>
    dataType.toUpperCase match {
      case "STRING" => value.toString
      case "INT" | "INTEGER" | "BIGINT" => if (v.isInstanceOf[BigInt]) v else BigInt(v.toString)
      case "DOUBLE" | "FLOAT" | "NUMBER" | "DECIMAL" => if (v.isInstanceOf[BigDecimal]) v else BigDecimal(v.toString)
      case "BOOLEAN" => toBoolean(value)
      case bad => throw PipelineException(
        message = Some(s"Illegal cast to type [$bad] in ${getClass.getSimpleName}"),
        pipelineProgress = pipelineContext.currentStateInfo
      )
    }
  }

  def castAttribute(attribute: Attribute, dataType: String): Attribute = dataType.toUpperCase match {
    case "STRING" | "BIGINT" | "DECIMAL" | "BOOLEAN" => attribute.copy(dataType = AttributeType(dataType))
    case "INT" | "INTEGER" => attribute.copy(dataType = AttributeType("BigInt"))
    case "DOUBLE" | "FLOAT" | "NUMBER" => attribute.copy(dataType = AttributeType("Decimal"))
    case bad => throw PipelineException(
      message = Some(s"Illegal cast to type [$bad] in ${getClass.getSimpleName}"),
      pipelineProgress = pipelineContext.currentStateInfo
    )
  }

  def literal(lit: Literal): Option[Any] = lit match {
    case BooleanLiteral(boolean) => Some(boolean.value.toBoolean)
    case DecimalLiteral(numeric) => Some(BigDecimal(numeric.value))
    case IntegerLiteral(numeric) => Some(BigInt(numeric.value))
    case NullLiteral(_) => None
    case l => Some(l.value) // string
  }

  def literalAttribute(lit: Literal): Attribute = lit match {
    case BooleanLiteral(_) => Attribute("", AttributeType("Boolean"), Some(false), None)
    case DecimalLiteral(_) => Attribute("", AttributeType("Decimal"), Some(false), None)
    case IntegerLiteral(_) => Attribute("", AttributeType("BigInt"), Some(false), None)
    case NullLiteral(_) => Attribute("", AttributeType("Null"), Some(true), None)
    case l => Attribute("", AttributeType("String"), Some(false), None)
  }

  private def identifier(ident: Identifier, row: String => Option[Any]): Option[Any] = ident.qualifiers.map { qualifiers =>
    val (value, list) = qualifiers.take(2) match {
      case List(a, f) if tableAlias.contains(a) => (row(s"$a.$f"), qualifiers.drop(2) :+ ident.value)
      case List(a) if tableAlias.contains(a) => (row(s"$a.${ident.value}"), Nil)
      case l => (row(l.head), l.drop(1) :+ ident.value)
    }
    value.map{
      case v if list.nonEmpty =>
        ReflectionUtils.extractField(v, list.mkString("."), extractFromOption = false) match {
          case o: Option[_] => o
          case v => Some(v)
        }
      case v => v
    }
  } getOrElse {
    row(ident.value)
  }

  private def identifierAttribute(ident: Identifier, indices: Map[String, Int]): Attribute = (ident.qualifiers.map { qualifiers =>
    val (index, list) = qualifiers.take(2) match {
      case List(a, f) if tableAlias.contains(a) => (indices.get(s"$a.$f"), qualifiers.drop(2) :+ ident.value)
      case List(a) if tableAlias.contains(a) => (indices.get(s"$a.${ident.value}"), Nil)
      case l => (indices.get(l.head), l.drop(1) :+ ident.value)
    }
    index.map(i => internalReference.schema.attributes(i)).map {
      case a if list.nonEmpty =>
        a // handle recursive later
      case a => a
    }
  } getOrElse {
    indices.get(ident.value).map(i => internalReference.schema.attributes(i))
      //.getOrElse(Attribute("", AttributeType("Null"), Some(true), None))
  }).getOrElse(Attribute(ident.value, AttributeType("Null"), Some(true), None))
}
