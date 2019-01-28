package com.acxiom.pipeline.steps

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
  def read(jdbcOptions: JDBCOptions,
                columns: String = "*",
                where: Option[String] = None,
                pipelineContext: PipelineContext): DataFrame = {

    val spark = pipelineContext.sparkSession.get

    val df = spark.read.format("jdbc")
      .options(jdbcOptions.asProperties.toMap)
      .load()

    if (where.isEmpty) {
      df.select(columns)
    }
    else {
      df.select(columns).where(where.get)
    }
  }

}
