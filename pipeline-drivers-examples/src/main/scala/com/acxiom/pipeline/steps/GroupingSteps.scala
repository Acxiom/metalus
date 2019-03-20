package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object GroupingSteps {
  @StepFunction("079b6053-1e05-54d8-84c2-020c96a440c8",
    "Counts By Field",
    "Returns counts by the provided field name. The result is a data frame.",
    "Pipeline",
  "Example")
  def countsByField(dataFrame: DataFrame, fieldName: String): DataFrame = {
    dataFrame.groupBy(fieldName).count()
  }

  @StepFunction("bcdd0f7c-0b2a-410d-9871-8400107046c3",
    "Record Count",
    "Returns number of records in the data frame.",
    "Pipeline",
    "Example")
  def recordCount(dataFrame: DataFrame): Long = {
    dataFrame.count()
  }

  @StepFunction("99ad5ed4-b907-5635-8f2a-1c9012f6f5a7",
    "Performs a grouping and aggregation of the data",
    "Performs a grouping across all columns in the DataFrame and aggregation using the groupByField of the data.",
    "Pipeline",
    "Example")
  def groupByField(dataFrame: DataFrame, groupByField: String): DataFrame = {
    dataFrame.groupBy(dataFrame.schema.fields.map(field => dataFrame(field.name)): _*).agg(dataFrame(groupByField))
  }
}
