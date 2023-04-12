package com.acxiom.metalus.connectors.jdbc

import com.acxiom.metalus.connectors.{DataConnector, DataRowReader, DataRowWriter, DataStreamOptions}
import com.acxiom.metalus.sql.jdbc.{BasicJDBCDataReference, JDBCDataReference, JDBCUtils}
import com.acxiom.metalus.sql.{Attribute, AttributeType, DataReferenceOrigin, Row, Schema}
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.{Constants, Credential, PipelineContext, PipelineException, UserNameCredential}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSetMetaData
import java.util.Date

case class JDBCDataConnector(url: String,
                             name: String,
                             credentialName: Option[String] = None,
                             credential: Option[Credential] = None,
                             defaultProperties: Option[Map[String, Any]] = None) extends DataConnector {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  override def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): JDBCDataReference[_] = {
    val dbtable = {
      logger.info(s"${properties}")
      val tmp = properties.flatMap(_.get("dbtable")).collect {
        case o: Option[_] => o.mkString
        case v => v.toString
      }.mkString
      logger.info(s"tmp is value = $tmp")
      if (tmp.isEmpty) {
        throw PipelineException(message = Some("dbtable must be provided to create a JDBCDataReference"),
          pipelineProgress = pipelineContext.currentStateInfo)
      }
      tmp
    }
    getTable(dbtable, properties.map(_.mapValues(_.toString).filterKeys(_ != "dbtable").toMap[String, String]), pipelineContext)
  }

  def getTable(dbtable: String, properties: Option[Map[String, String]], pipelineContext: PipelineContext): JDBCDataReference[_] =
    getTable(() => dbtable, properties, pipelineContext)

  def getTable(dbtable: () => String, properties: Option[Map[String, String]], pipelineContext: PipelineContext): JDBCDataReference[_] = {
    val info = defaultProperties.getOrElse(Map()).mapValues(_.toString).toMap[String, String] ++
      properties.getOrElse(Map()) ++
      getCredential(pipelineContext).collect {
        case unc: UserNameCredential => Map("user" -> unc.name, "password" -> unc.password)
      }.getOrElse(Map())
    BasicJDBCDataReference(dbtable, url, info, DataReferenceOrigin(this, Some(info)), pipelineContext)
  }

  /**
   * Returns a DataRowReader or None. The reader can be used to window data from the connector.
   *
   * @param properties      Optional properties required by the reader.
   * @param pipelineContext The current PipelineContext
   * @return Returns a DataRowReader or None.
   */
  override def getReader(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowReader] = {
    if (properties.isEmpty || (!properties.get.options.contains("dbtable") && !properties.get.options.contains("sql"))) {
      throw DriverUtils.buildPipelineException(Some("Either the dbtable or sql property is required!"), None, Some(pipelineContext))
    }
    val dbTable = properties.get.options.get("dbtable")
    val sql = properties.get.options.get("sql")
    val finalSql = if (sql.isEmpty) {
      dbTable.get
    } else {
      sql.get.toString
    }
    Some(JDBCDataRowReader(this, properties.get.copy(options = properties.get.options + ("dbtable" -> finalSql)), pipelineContext))
  }

  /**
   * Returns a DataRowWriter or None. The writer can be used to window data to the connector.
   *
   * @param properties      Optional properties required by the writer.
   * @param pipelineContext The current PipelineContext
   * @return Returns a DataRowWriter or None.
   */
  override def getWriter(properties: Option[DataStreamOptions], pipelineContext: PipelineContext): Option[DataRowWriter] = {
    if (properties.isEmpty || !properties.get.options.contains("dbtable")) {
      throw DriverUtils.buildPipelineException(Some("The dbtable property is required!"), None, Some(pipelineContext))
    }
    Some(JDBCDataRowWriter(this, properties.get.options("dbtable").toString, properties.get, pipelineContext))
  }
}

private case class JDBCDataRowReader(connector: JDBCDataConnector,
                                     properties: DataStreamOptions,
                                     pipelineContext: PipelineContext) extends DataRowReader {
  private lazy val dataRef = connector.createDataReference(Some(properties.options), pipelineContext).asInstanceOf[BasicJDBCDataReference]
  private lazy val result = dataRef.execute
  private lazy val resultSet = result.resultSet.get
  private lazy val schema = {
    val metadata = resultSet.getMetaData
    val count = metadata.getColumnCount
    Schema(Range(Constants.ONE, count + Constants.ONE).map(index => {
      Attribute(metadata.getColumnName(index), AttributeType(metadata.getColumnTypeName(index)),
        Some(metadata.isNullable(index) match {
          case ResultSetMetaData.columnNoNulls => false
          case _ => true
        }), None)
    }))
  }
  /**
   * Fetches the next set of rows from the stream. An empty list indicates the stream is open but no data was available
   * while None indicates the stream is closed and no more data is available,
   *
   * @return A list of rows or None if the end of the stream has been reached.
   */
  override def next(): Option[List[Row]] = readDataWindow(properties, (list, index) => {
    if (resultSet.next()) {
      val cols = schema.attributes.map(col => {
        col.dataType.baseType.toLowerCase match{
          case "int" | "integer" | "bigint" => resultSet.getInt(col.name)
          case "long" => resultSet.getLong(col.name)
          case "float" => resultSet.getFloat(col.name)
          case "bigdecimal" => resultSet.getBigDecimal(col.name)
          case "double" => resultSet.getDouble(col.name)
          case "boolean" => resultSet.getBoolean(col.name)
          case "byte" => resultSet.getByte(col.name)
          case "date" => resultSet.getDate(col.name)
          case "string" => resultSet.getString(col.name)
          case _ => resultSet.getObject(col.name)
        }
      }).toArray
      list :+ Row(cols, Some(schema), None)
    } else {
      list
    }
  })

  /**
   * Closes the stream.
   */
  override def close(): Unit = {
    resultSet.close()
    result.connection.foreach(c => c.close())
  }

  /**
   * Opens the stream for processing.
   */
  override def open(): Unit = {}
}

private case class JDBCDataRowWriter(connector: JDBCDataConnector,
                                     tableName: String,
                                     properties: DataStreamOptions,
                                     pipelineContext: PipelineContext) extends DataRowWriter {

  private lazy val connection = JDBCUtils.createConnection(connector.url, properties.options.mapValues(_.toString).toMap[String, String])

  /**
   * Prepares the provided rows and pushes to the stream. The format of the data will be determined by the
   * implementation.
   *
   * @param rows A list of Row objects.
   * @throws PipelineException - will be thrown if this call cannot be completed.
   */
  override def process(rows: List[Row]): Unit = {
    if (rows.nonEmpty) {
      val row = rows.head
      val schema = getSchema(row)
      val sqlValues = schema.attributes.foldLeft((List[String](), List[String]()))((sql, a) => {
        (sql._1 :+ a.name, sql._2 :+ "?")
      })
      val finalSql = s"INSERT INTO $tableName (${sqlValues._1.mkString(",")}) VALUES (${sqlValues._2.mkString(",")})"
      val statement = connection.prepareStatement(finalSql)
      rows.foreach { row =>
        row.columns.zipWithIndex.foreach { value =>
          val index = value._2 + 1
          value._1 match {
            case i: Int => statement.setInt(index, i)
            case i: Integer => statement.setInt(index, i)
            case l: Long => statement.setLong(index, l)
            case f: Float => statement.setFloat(index, f)
            case d: BigDecimal => statement.setBigDecimal(index, d.bigDecimal)
            case d: Double => statement.setDouble(index, d)
            case b: Boolean => statement.setBoolean(index, b)
            case b: Byte => statement.setByte(index, b)
            case d: Date => statement.setDate(index, new java.sql.Date(d.getTime))
            case s: String => statement.setString(index, s)
            case _ => statement.setObject(index, value._1)
          }
        }
        statement.executeUpdate()
      }
      statement.close()
    }
  }

  private def getSchema(row: Row) = {
    if (properties.schema.isDefined) {
      properties.schema.get
    } else if (properties.options.contains("schema")) {
      properties.options("schema").asInstanceOf[Schema]
    } else if (row.schema.isDefined) {
      row.schema.get
    } else {
      throw DriverUtils.buildPipelineException(Some("A valid Schema is required!"), None, Some(pipelineContext))
    }
  }

  /**
   * Closes the stream.
   */
  override def close(): Unit = {
    connection.close()
  }

  /**
   * Opens the stream for processing.
   */
  override def open(): Unit = {}
}
