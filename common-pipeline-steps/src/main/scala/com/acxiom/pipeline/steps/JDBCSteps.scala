package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.JavaConversions._

@StepObject
object JDBCSteps {

  @StepFunction("cdb332e3-9ea4-4c96-8b29-c1d74287656c",
    "Load table as DataFrame",
    "This step will load a table from the provided jdbc information",
    "Pipeline")
  def readWithJDBCOptions(jdbcOptions: JDBCOptions,
                          pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get

    spark.read.format("jdbc")
      .options(jdbcOptions.asProperties.toMap)
      .load()
  }

  @StepFunction("72dbbfc8-bd1d-4ce4-ab35-28fa8385ea54",
    "Load table as DataFrame",
    "This step will load a table from the provided jdbc information",
    "Pipeline")
  def readWithStepOptions(jDBCStepsOptions: JDBCStepsOptions,
                          pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get
    val map = jDBCStepsOptions.connectionProperties.getOrElse(Map[String, String]())
    val properties = (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }

    spark.read.jdbc(
      url = jDBCStepsOptions.url,
      table = jDBCStepsOptions.table,
      predicates = jDBCStepsOptions.predicates.getOrElse(Array[String]()),
      connectionProperties = properties
    )
  }

  @StepFunction("dcc57409-eb91-48c0-975b-ca109ba30195",
    "Load table as DataFrame",
    "This step will load a table from the provided jdbc information",
    "Pipeline")
  def readWithProperties(url: String,
                         table: String,
                         predicates: Array[String] = Array[String](),
                         connectionProperties: Option[Map[String, String]] = None,
                         pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get
    val map = connectionProperties.getOrElse(Map[String, String]())
    val properties = (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }

    spark.read.jdbc(url, table, predicates, properties)
  }

  @StepFunction("c9fddf52-34b1-4216-a049-10c33ccd24ab",
    "Write DataFrame to JDBC",
    "This step will write a DataFrame as a table using JDBC",
    "Pipeline")
  def writeWithJDBCOptions(dataFrame: DataFrame,
                           jdbcOptions: JDBCOptions,
                           saveMode: String = "Overwrite"): Unit = {
    dataFrame.write.format("jdbc")
      .mode(saveMode)
      .options(jdbcOptions.asProperties.toMap)
      .save()
  }

  @StepFunction("77ffcd02-fbd0-4f79-9b35-ac9dc5fb7190",
    "Write DataFrame to JDBC",
    "This step will write a DataFrame as a table using JDBC",
    "Pipeline")
  def writeWithProperties(dataFrame: DataFrame,
                          url: String,
                          table: String,
                          connectionProperties: Option[Map[String, String]] = None,
                          saveMode: String = "Overwrite"): Unit = {
    val map = connectionProperties.getOrElse(Map[String, String]())
    val properties = (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
    dataFrame.write
      .mode(saveMode)
      .jdbc(url, table, properties)
  }

  @StepFunction("3d6b77a1-52c2-49ba-99a0-7ec773dac696",
    "Write DataFrame to JDBC",
    "This step will write a DataFrame as a table using JDBC",
    "Pipeline")
  def writeWithStepOptions(dataFrame: DataFrame,
                           jDBCStepsOptions: JDBCStepsOptions,
                           saveMode: String = "Overwrite"): Unit = {
    dataFrame.write
      .mode(saveMode)
      .format("jdbc")
      .option("url", jDBCStepsOptions.url)
      .option("dbtable", jDBCStepsOptions.table)
      .options(jDBCStepsOptions.connectionProperties.getOrElse(Map[String, String]()))
  }

}

/**
  *
  * @param url                  A valid jdbc url.
  * @param table                A table name or subquery.
  * @param predicates           Optional predicate used for partitioning.
  * @param connectionProperties Optional JDBC properties.
  */
case class JDBCStepsOptions(url: String,
                            table: String,
                            predicates: Option[Array[String]] = None,
                            connectionProperties: Option[Map[String, String]] = None)
