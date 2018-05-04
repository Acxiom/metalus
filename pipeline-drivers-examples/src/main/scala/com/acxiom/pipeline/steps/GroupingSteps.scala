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
}
