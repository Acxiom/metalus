package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.DataFrame

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
  def writeDataFrame(dataFrame: DataFrame, table: String, options: Option[DataFrameWriterOptions] = None): Unit = {
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions())).saveAsTable(table)
  }

  @StepFunction("5874ab64-13c7-404c-8a4f-67ff3b0bc7cf",
    "Drop Hive Object",
    "This step will drop on object from the hive meta store",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("name" -> StepParameter(None, Some(true), description = Some("Name of the object to drop")),
    "objectType" -> StepParameter(None, Some(false), Some("TABLE"), description = Some("Type of object to drop")),
    "ifExists" -> StepParameter(None, Some(false), Some("false"), description = Some("Flag to control whether existence is checked")),
    "cascade" -> StepParameter(None, Some(false), Some("false"), description = Some("Flag to control whether a this operation should cascade"))))
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
}
