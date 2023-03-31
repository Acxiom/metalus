package com.acxiom.metalus.connectors
import com.acxiom.metalus.{Credential, PipelineContext, PipelineException, sql}
import com.acxiom.metalus.sql.{TablesawDataFrame, DataReference, DataReferenceOrigin, InMemoryDataReference, Row, Schema}
import tech.tablesaw.api._
import tech.tablesaw.io.ReadOptions
import tech.tablesaw.io.csv.CsvReadOptions
import tech.tablesaw.io.fixed.FixedWidthReadOptions

import java.io.InputStream


case class InMemoryDataConnector(name: String,
                                 credentialName: Option[String] = None,
                                 credential: Option[Credential] = None) extends DataConnector {
  override def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): DataReference[_] = {
    val props = properties.getOrElse(Map())
    val schema = props.get("schema").collect{ case s: Schema => s}
    props.get("path")
      .map(_.toString)
      .map(loadStream(_, schema, properties, pipelineContext))
      .orElse {
        val origin = DataReferenceOrigin(this, properties)
          props.get("data").collect {
            case seq: Seq[_] if seq.headOption.exists(_.isInstanceOf[Row]) =>
              InMemoryDataReference(() => fromList(seq.asInstanceOf[Seq[Row]].toList, schema), origin, pipelineContext)
            case stream: java.io.InputStream =>
              InMemoryDataReference(() => fromStream(stream, schema, properties), origin, pipelineContext)
          }
      }.getOrElse(
      throw PipelineException(
        message = Some("path or data options must be provided to create an InMemoryDataReference"),
        pipelineProgress = pipelineContext.currentStateInfo
      )
    )
  }

  private def fromList(rows: List[Row], schema: Option[Schema]): TablesawDataFrame = {
    val table = Table.create(name)
    val finalSchema = schema.orElse(rows.headOption.flatMap(_.schema)).get
    val colFunctions = finalSchema.attributes.zipWithIndex.map { case (a, i) =>
      a.dataType.baseType.toLowerCase match {
        case "int" | "integer" =>
          val c = IntColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.asInstanceOf[Integer])).getOrElse(c.setMissing(rowIndex))
        case "long" =>
          val c = LongColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.asInstanceOf[java.lang.Long])).getOrElse(c.setMissing(rowIndex))
        case "double" =>
          val c = DoubleColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.asInstanceOf[java.lang.Double])).getOrElse(c.setMissing(rowIndex))
        case "float" =>
          val c = FloatColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.asInstanceOf[java.lang.Float])).getOrElse(c.setMissing(rowIndex))
        case "boolean" =>
          val c = BooleanColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.asInstanceOf[java.lang.Boolean])).getOrElse(c.setMissing(rowIndex))
        case "text" =>
          val c = TextColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.toString)).getOrElse(c.setMissing(rowIndex))
        case "string" =>
          val c = StringColumn.create(a.name, rows.length)
          table.addColumns(c)
          (rowIndex: Int, r: Row) =>
            r.get(i).flatMap(Option(_)).map(v => c.set(rowIndex, v.toString)).getOrElse(c.setMissing(rowIndex))
      }
    }
    rows.zipWithIndex.foreach { case (row, i) =>
      colFunctions.foreach(setValue => setValue(i, row))
    }
    TablesawDataFrame(table)
  }

  private def fromStream(in: InputStream, schema: Option[Schema], properties: Option[Map[String, Any]]): TablesawDataFrame = {
    val options = properties.map(_.mapValues(_.toString).toMap).getOrElse(Map())
    TablesawDataFrame(Table.read().usingOptions(getOptions(in, schema, options)))
  }

  private def getOptions(in: InputStream, schema: Option[Schema], options: Map[String, String]): ReadOptions = {
    val columnTypes = schema.map(schemaToColumTypes)
    options.getOrElse("format", "csv").toLowerCase match {
      case "csv" =>
        val builder = CsvReadOptions.builder(in).tableName(name)
        val withSchema = columnTypes.map(builder.columnTypes).getOrElse(builder)
        val withHeader = options.get("header").map(h => withSchema.header(h.toBoolean)).getOrElse(withSchema)
        options.get("delimiter").orElse(options.get("sep")).map(s => withHeader.separator(s.head)).getOrElse(withHeader)
        .build()
      case "fixed" =>
        val builder = FixedWidthReadOptions.builder(in).tableName(name)
        val withSchema = columnTypes.map(builder.columnTypes).getOrElse(builder)
        val withHeader = options.get("header").map(h => withSchema.header(h.toBoolean)).getOrElse(withSchema)
        val withPadding = options.get("padding").map(p => withHeader.padding(p.head)).getOrElse(withHeader)
        withPadding.build()
    }
  }

  private def schemaToColumTypes(schema: Schema): Array[ColumnType] = schema.attributes.map{ a =>
    a.dataType.baseType.toLowerCase match {
      case "int" | "integer" => ColumnType.INTEGER
      case "long" => ColumnType.LONG
      case "double" => ColumnType.DOUBLE
      case "float" => ColumnType.FLOAT
      case "boolean" => ColumnType.BOOLEAN
      case "text" => ColumnType.TEXT
      case "string" => ColumnType.STRING
    }
  }.toArray


  def loadStream(path: String,
                 schema: Option[Schema],
                 properties: Option[Map[String, Any]], pipelineContext: PipelineContext): InMemoryDataReference = {
    val options: Map[String, Any] = properties.getOrElse(Map()) ++
      credentialName.map("credentialName" -> _) ++ credential.map("credential" -> _)

    val getTable: () => TablesawDataFrame = { () =>
      val conn = ConnectorProvider.getConnector("loadInMemory", path, Some("FILE"), Some(options))
      if (conn.isEmpty) {
        throw new IllegalArgumentException(s"Unable to connector connector for path: $path")
      }
      val in = conn.get.asInstanceOf[FileConnector].getFileManager(pipelineContext).getFileResource(path).getInputStream()
      val df = fromStream(in, schema, properties)
      in.close()
      df
    }
    InMemoryDataReference(getTable, DataReferenceOrigin(this, if(options.nonEmpty) Some(options) else None), pipelineContext)
  }
}
