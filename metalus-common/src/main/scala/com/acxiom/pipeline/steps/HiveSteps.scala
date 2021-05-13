package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import org.apache.spark.sql.{DataFrame, Dataset}

@deprecated("Use CatalogSteps")
object HiveSteps {

  @deprecated("Use CatalogSteps.readDataFrame")
  def readDataFrame(table: String, options: Option[DataFrameReaderOptions] = None, pipelineContext: PipelineContext): DataFrame ={
    CatalogSteps.readDataFrame(table, options, pipelineContext)
  }

  @deprecated("Use CatalogSteps.writeDataFrame")
  def writeDataFrame(dataFrame: Dataset[_], table: String, options: Option[DataFrameWriterOptions] = None): Unit = {
    CatalogSteps.writeDataFrame(dataFrame, table, options)
  }

  @deprecated("Use CatalogSteps.drop")
  def drop(name: String,
           objectType: Option[String] = None,
           ifExists: Option[Boolean] = None,
           cascade: Option[Boolean] = None,
           pipelineContext: PipelineContext): DataFrame = {
    CatalogSteps.drop(name, objectType, ifExists, cascade, pipelineContext)
  }

  @deprecated("Use CatalogSteps.databaseExists")
  def databaseExists(name: String, pipelineContext: PipelineContext): Boolean = {
    CatalogSteps.databaseExists(name, pipelineContext)
  }

  @deprecated("Use CatalogSteps.tableExists")
  def tableExists(name: String, database: Option[String] = None, pipelineContext: PipelineContext): Boolean = {
    CatalogSteps.tableExists(name, database, pipelineContext)
  }

  @deprecated("Use CatalogSteps.setCurrentDatabase")
  def setCurrentDatabase(name: String, pipelineContext: PipelineContext): Unit = {
    CatalogSteps.setCurrentDatabase(name, pipelineContext)
  }

  @deprecated("Use CatalogSteps.createTable")
  def createTable(name: String, externalPath: Option[String] = None,
                  options: Option[DataFrameReaderOptions] = None,
                  pipelineContext: PipelineContext): DataFrame = {
    CatalogSteps.createTable(name, externalPath, options, pipelineContext)
  }
}
