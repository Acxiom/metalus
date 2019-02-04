package com.acxiom.pipeline.steps

import java.util.Properties

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.JavaConversions.propertiesAsScalaMap

@StepObject
object JDBCSteps {

  @StepFunction("cdb332e3-9ea4-4c96-8b29-c1d74287656c",
    "Load table as DataFrame",
    "This step will load a table from the provided jdbc information",
    "Pipeline")
  def readWithJDBCOptions(jdbcOptions: JDBCOptions,
                          columns: List[String] = List[String]("*"),
                          where: Option[String] = None,
                          pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get

    val df = spark.read.format("jdbc")
      .options(jdbcOptions.asProperties.toMap)
      .load()

    if (where.isEmpty) {
      df.selectExpr(columns:_*)
    } else {
      df.selectExpr(columns:_*).where(where.get)
    }
  }

  @StepFunction("dcc57409-eb91-48c0-975b-ca109ba30195",
    "Load table as DataFrame",
    "This step will load a table from the provided jdbc information",
    "Pipeline")
  def readWithProperties(url: String,
                         table: String,
                         predicates: Array[String] = Array[String](),
                         connectionProperties: Properties = new Properties(),
                         columns: List[String] = List[String]("*"),
                         where: Option[String] = None,
                         pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get

    val df = spark.read.jdbc(url, table, predicates, connectionProperties)

    if (where.isEmpty) {
      df.selectExpr(columns: _*)
    } else {
      df.selectExpr(columns: _*).where(where.get)
    }
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
                          connectionProperties: Properties = new Properties(),
                          saveMode: String = "Overwrite"): Unit = {
    dataFrame.write
      .mode(saveMode)
      .jdbc(url, table, connectionProperties)
  }
}

case class JDBCStepsOptions(url: String,
                            table: String,
                            predicates: Array[String] = Array[String](),
                            connectionProperties: Properties = new Properties())