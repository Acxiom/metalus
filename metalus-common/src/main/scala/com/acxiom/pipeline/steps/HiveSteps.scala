package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

@StepObject
object HiveSteps {

  @StepFunction("3806f23b-478c-4054-b6c1-37f11db58d38",
    "Read a DataFrame from Hive",
    "This step will read a dataFrame in a given format from Hive",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("table" -> StepParameter(None, Some(true), description = Some("The name of the table to read")),
    "options" -> StepParameter(None, Some(false), description = Some("The DataFrameReaderOptions to use"))))
  def readDataFrame(table: String, options: Option[DataFrameReaderOptions] = None, pipelineContext: PipelineContext): DataFrame ={
    DataFrameSteps.getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext).table(table)
  }

  @StepFunction("e2b4c011-e71b-46f9-a8be-cf937abc2ec4",
    "Write DataFrame to Hive",
    "This step will write a dataFrame in a given format to Hive",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to write")),
    "table" -> StepParameter(None, Some(true), description = Some("The name of the table to write to")),
    "options" -> StepParameter(None, Some(false), description = Some("The DataFrameWriterOptions to use"))))
  def writeDataFrame(dataFrame: Dataset[_], table: String, options: Option[DataFrameWriterOptions] = None): Unit = {
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions())).saveAsTable(table)
  }

  @StepFunction("5874ab64-13c7-404c-8a4f-67ff3b0bc7cf",
    "Drop Hive Object",
    "This step will drop an object from the hive meta store",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("name" -> StepParameter(None, Some(true), description = Some("Name of the object to drop")),
    "objectType" -> StepParameter(None, Some(false), Some("TABLE"), description = Some("Type of object to drop")),
    "ifExists" -> StepParameter(None, Some(false), Some("false"), description = Some("Flag to control whether existence is checked")),
    "cascade" -> StepParameter(None, Some(false), Some("false"), description = Some("Flag to control whether this deletion should cascade"))))
  def drop(name: String,
           objectType: Option[String] = None,
           ifExists: Option[Boolean] = None,
           cascade: Option[Boolean] = None,
           pipelineContext: PipelineContext): DataFrame = {
    val cascadable = List("SCHEMA", "DATABASE")
    val objectName = objectType.getOrElse("TABLE")
    val ifString = ifExists.collect { case true => "IF EXISTS" }.getOrElse("")
    val cascadeString = cascade.collect { case true if cascadable.contains(objectName.toUpperCase) => "CASCADE" }.getOrElse("")
    val sql = s"DROP $objectName $ifString $name $cascadeString"
    pipelineContext.sparkSession.get.sql(sql)
  }

  @StepFunction("17be71f9-1492-4404-a355-1cc973694cad",
    "Database Exists",
    "Check spark catalog for a database with the given name.",
    "branch",
    "Decision")
  @StepParameters(Map("name" -> StepParameter(None, Some(true), description = Some("Name of the database"))))
  def databaseExists(name: String, pipelineContext: PipelineContext): Boolean = {
    pipelineContext.sparkSession.get.catalog.databaseExists(name)
  }

  @StepFunction("95181811-d83e-4136-bedb-2cba1de90301",
    "Table Exists",
    "Check spark catalog for a table with the given name.",
    "branch",
    "Decision")
  @StepParameters(Map(
    "name" -> StepParameter(None, Some(true), description = Some("Name of the table")),
    "database" -> StepParameter(None, Some(false), description = Some("Name of the database"))))
  def tableExists(name: String, database: Option[String] = None, pipelineContext: PipelineContext): Boolean = {
    if (database.isDefined) {
      pipelineContext.sparkSession.get.catalog.tableExists(database.get, name)
    } else {
      pipelineContext.sparkSession.get.catalog.tableExists(name)
    }
  }

  @StepFunction("f4adfe70-2ae3-4b8d-85d1-f53e91c8dfad",
    "Set Current Database",
    "Set the current default database for the spark session.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("name" -> StepParameter(None, Some(true), description = Some("Name of the database"))))
  def setCurrentDatabase(name: String, pipelineContext: PipelineContext): Unit = {
    pipelineContext.sparkSession.get.catalog.setCurrentDatabase(name)
  }

  @StepFunction("663f8c93-0a42-4c43-8263-33f89c498760",
    "Create Table",
    "Create Hive Table.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map(
    "name" -> StepParameter(None, Some(true), description = Some("Name of the table")),
    "externalPath" -> StepParameter(None, Some(false), description = Some("Path of the external table")),
    "options" -> StepParameter(None, Some(false), description = Some("Options containing the format, schema, and settings"))))
  def createTable(name: String, externalPath: Option[String] = None,
                  options: Option[DataFrameReaderOptions] = None,
                  pipelineContext: PipelineContext): DataFrame = {
    val dfOptions = options.getOrElse(DataFrameReaderOptions("hive"))
    val finalOptions = dfOptions.options.getOrElse(Map[String, String]()) ++ externalPath.map(p => "path" -> p)
    val schema = dfOptions.schema.map(_.toStructType()).getOrElse(new StructType)
    pipelineContext.sparkSession.get.catalog.createTable(name, dfOptions.format, schema, finalOptions)
  }
}
