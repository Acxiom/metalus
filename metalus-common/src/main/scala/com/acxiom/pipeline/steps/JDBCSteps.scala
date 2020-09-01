package com.acxiom.pipeline.steps

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.JavaConversions._

@StepObject
object JDBCSteps {
  private val connectionPropertiesDesc: Some[String] = Some("Optional properties for the jdbc connection")
  private val optionsDescription: Some[String] = Some("The options to use when loading the DataFrameReader")
  private val urlDesc: Some[String] = Some("A valid jdbc url")
  private val tableDesc: Some[String] = Some("A table name or subquery")
  private val dfWriteDesc: Some[String] = Some("The DataFrame to be written")
  private val saveModeDesc: Some[String] = Some("The value for the mode option. Defaulted to Overwrite")

  /**
    * Read a table into a DataFrame via JDBC using a spark JDBCOptions object.
    *
    * @param jdbcOptions Options for configuring the jdbc connection.
    * @return A DataFrame representing the imported table.
    */
  @StepFunction("cdb332e3-9ea4-4c96-8b29-c1d74287656c",
    "Load table as DataFrame using JDBCOptions",
    "This step will load a table from the provided JDBCOptions",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("jdbcOptions" -> StepParameter(None, Some(true), None, None, None, None, Some("The options to use when loading the DataFrame"))))
  def readWithJDBCOptions(jdbcOptions: JDBCOptions,
                          pipelineContext: PipelineContext): DataFrame = {
    val options = DataFrameReaderOptions("jdbc", Some(jdbcOptions.asProperties.toMap))
    DataFrameSteps.getDataFrameReader(options, pipelineContext).load()
  }

  /**
    * Read a table into a DataFrame using jdbc.
    * The JDBCDataFrameReaderOptions allows for more options to be provided to the underlying DataFrameReader.
    *
    * @param jDBCStepsOptions Options for the JDBC connect and spark DataFrameReader.
    * @return A DataFrame representing the imported table.
    */
  @StepFunction("72dbbfc8-bd1d-4ce4-ab35-28fa8385ea54",
    "Load table as DataFrame using StepOptions",
    "This step will load a table from the provided JDBCDataFrameReaderOptions",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("jDBCStepsOptions" -> StepParameter(None, Some(true), None, None, None, None, optionsDescription)))
  def readWithStepOptions(jDBCStepsOptions: JDBCDataFrameReaderOptions,
                          pipelineContext: PipelineContext): DataFrame = {
    val properties = new Properties()
    properties.putAll(jDBCStepsOptions.readerOptions.options.getOrElse(Map[String, String]()))
    val reader = DataFrameSteps.getDataFrameReader(jDBCStepsOptions.readerOptions, pipelineContext)
    if (jDBCStepsOptions.predicates.isDefined) {
      reader.jdbc(
        url = jDBCStepsOptions.url,
        table = jDBCStepsOptions.table,
        predicates = jDBCStepsOptions.predicates.get.toArray,
        connectionProperties = properties
      )
    } else {
      reader.jdbc(
        url = jDBCStepsOptions.url,
        table = jDBCStepsOptions.table,
        properties = properties
      )
    }
  }

  /**
    * Read a table into a DataFrame via JDBC.
    *
    * @param url                  A valid jdbc url.
    * @param table                A table name or subquery.
    * @param predicates           Optional predicates used for partitioning.
    * @param connectionProperties Optional properties for the jdbc connection.
    * @return A DataFrame representing the imported table.
    */
  @StepFunction("dcc57409-eb91-48c0-975b-ca109ba30195",
    "Load table as DataFrame",
    "This step will load a table from the provided jdbc information",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("url" -> StepParameter(None, Some(true), None, None, None, None, urlDesc),
    "table" -> StepParameter(None, Some(true), None, None, None, None, tableDesc),
    "predicates" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional predicates used for partitioning")),
    "connectionProperties" -> StepParameter(None, Some(false), None, None, None, None, connectionPropertiesDesc)))
  def readWithProperties(url: String,
                         table: String,
                         predicates: Option[List[String]] = None,
                         connectionProperties: Option[Map[String, String]] = None,
                         pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get
    val properties = new Properties()
    properties.putAll(connectionProperties.getOrElse(Map[String, String]()))

    if (predicates.isDefined) {
      spark.read.jdbc(url, table, predicates.get.toArray, properties)
    } else {
      spark.read.jdbc(url, table, properties)
    }
  }

  /**
    * Write a DataFrame to a table via JDBC using a spark JDBCOptions object.
    * @param dataFrame   The DataFrame to be written.
    * @param jdbcOptions Options for configuring the JDBC connection.
    * @param saveMode    The value for the "mode" option. Defaulted to Overwrite.
    */
  @StepFunction("c9fddf52-34b1-4216-a049-10c33ccd24ab",
    "Write DataFrame to table using JDBCOptions",
    "This step will write a DataFrame as a table using JDBCOptions",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to be written")),
    "jdbcOptions" -> StepParameter(None, Some(true), None, None, None, None, Some("Options for configuring the JDBC connection")),
    "saveMode" -> StepParameter(None, Some(false), None, None, None, None, saveModeDesc)))
  def writeWithJDBCOptions(dataFrame: DataFrame,
                           jdbcOptions: JDBCOptions,
                           saveMode: String = "Overwrite"): Unit = {
    val options = DataFrameWriterOptions("jdbc", saveMode, Some(jdbcOptions.asProperties.toMap))
    DataFrameSteps.getDataFrameWriter(dataFrame, options).save()
  }

  /**
    * Write a DataFrame to a table via JDBC.
    *
    * @param dataFrame            The DataFrame to be written.
    * @param url                  A valid jdbc url.
    * @param table                A table name or subquery.
    * @param connectionProperties Optional properties for the jdbc connection.
    * @param saveMode             The value for the "mode" option. Defaulted to Overwrite.
    */
  @StepFunction("77ffcd02-fbd0-4f79-9b35-ac9dc5fb7190",
    "Write DataFrame to table",
    "This step will write a DataFrame to a table using the provided properties",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, dfWriteDesc),
    "url" -> StepParameter(None, Some(true), None, None, None, None, urlDesc),
    "table" -> StepParameter(None, Some(true), None, None, None, None, tableDesc),
    "connectionProperties" -> StepParameter(None, Some(false), None, None, None, None, connectionPropertiesDesc),
    "saveMode" -> StepParameter(None, Some(false), None, None, None, None, saveModeDesc)))
  def writeWithProperties(dataFrame: DataFrame,
                          url: String,
                          table: String,
                          connectionProperties: Option[Map[String, String]] = None,
                          saveMode: String = "Overwrite"): Unit = {
    val properties = new Properties()
    properties.putAll(connectionProperties.getOrElse(Map[String, String]()))
    dataFrame.write
      .mode(saveMode)
      .jdbc(url, table, properties)
  }

  /**
    * Write a DataFrame to a table via JDBC.
    * The JDBCDataFrameWriterOptions allows for more options to be provided to the underlying DataFrameReader.
    * @param dataFrame        The DataFrame to be written.
    * @param jDBCStepsOptions Options for the JDBC connect and spark DataFrameWriter.
    */
  @StepFunction("3d6b77a1-52c2-49ba-99a0-7ec773dac696",
    "Write DataFrame to JDBC table",
    "This step will write a DataFrame to a table using the provided JDBCDataFrameWriterOptions",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, dfWriteDesc),
    "jDBCStepsOptions" -> StepParameter(None, Some(true), None, None, None, None, Some("Options for the JDBC connect and spark DataFrameWriter"))))
  def writeWithStepOptions(dataFrame: DataFrame,
                           jDBCStepsOptions: JDBCDataFrameWriterOptions): Unit = {
    val properties = new Properties()
    properties.putAll(jDBCStepsOptions.writerOptions.options.getOrElse(Map[String, String]()))
    DataFrameSteps.getDataFrameWriter(dataFrame, jDBCStepsOptions.writerOptions)
      .jdbc(jDBCStepsOptions.url, jDBCStepsOptions.table, properties)
  }

  @StepFunction("713fff3d-d407-4970-89ae-7844e6fc60e3",
    "Get JDBC Connection",
    "Get a jdbc connection.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("url"-> StepParameter(None, Some(true), None, None, None, None, urlDesc),
    "properties" -> StepParameter(None, Some(false), None, None, None, None, connectionPropertiesDesc)))
  def getConnection(url: String, properties: Option[Map[String, String]] = None): Connection = {
    val prop = new Properties()
    if (properties.isDefined) prop.putAll(properties.get)
    DriverManager.getConnection(url, prop)
  }

  @StepFunction("549828be-3d96-4561-bf94-7ad420f9d203",
    "Execute Sql",
    "Execute a sql command using jdbc.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("sql"-> StepParameter(None, Some(true), None, None, None, None, Some("Sql command to execute")),
    "connection" -> StepParameter(None, Some(true), None, None, None, None, Some("An open jdbc connection")),
  "parameters" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional list of bind variables"))))
  def executeSql(sql: String, connection: Connection, parameters: Option[List[Any]] = None): PipelineStepResponse = {
    val p = connection.prepareStatement(sql)
    if (parameters.isDefined) {
      parameters.get.zipWithIndex.foreach{ case (a, i) => p.setObject(i + 1, a)}
    }
    val result = if (p.execute()) {
      val rs = p.getResultSet
      val columnNames = (1 to rs.getMetaData.getColumnCount).map(x => rs.getMetaData.getColumnName(x)).toList
      val records = Iterator.continually(rs.next())
        .takeWhile(identity)
        .map(_ => columnNames.map(n => n -> rs.getObject(n)).toMap)
        .toList
      PipelineStepResponse(Some(records), Some(Map("count" -> records.length)))
    } else {
      PipelineStepResponse(Some(Map()), Some(Map("count" -> p.getUpdateCount)))
    }
    p.close()
    result
  }

  @StepFunction("9c8957a3-899e-4f32-830e-d120b1917aa1",
    "Close JDBC Connection",
    "Close a JDBC Connection.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("connection" -> StepParameter(None, Some(true), None, None, None, None, Some("An open jdbc connection"))))
  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }

}

/**
  *
  * @param url                  A valid jdbc url.
  * @param table                A table name or subquery.
  * @param predicates           Optional predicates used for partitioning.
  * @param readerOptions        Optional DataFrameReader properties
  */
case class JDBCDataFrameReaderOptions(url: String,
                                      table: String,
                                      predicates: Option[List[String]] = None,
                                      readerOptions: DataFrameReaderOptions = DataFrameReaderOptions("jdbc"))

/**
  *
  * @param url                  A valid jdbc url.
  * @param table                A table name or subquery.
  * @param writerOptions        Optional DataFrameWriter properties
  */
case class JDBCDataFrameWriterOptions(url: String,
                                      table: String,
                                      writerOptions: DataFrameWriterOptions = DataFrameWriterOptions("jdbc"))
