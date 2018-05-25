package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object GroupingSteps {
  @StepFunction("079b6053-1e05-54d8-84c2-020c96a440c8",
    "Counts By Field",
    "Returns counts by the provided field name. The result is a data frame.",
    "Pipeline")
  def countsByField(dataFrame: DataFrame, fieldName: String): DataFrame = {
    dataFrame.groupBy(fieldName).count()
  }

  @StepFunction("bcdd0f7c-0b2a-410d-9871-8400107046c3",
    "Record Count",
    "Returns number of records in the data frame.",
    "Pipeline")
  def recordCount(dataFrame: DataFrame): Long = {
    dataFrame.count()
  }
}
