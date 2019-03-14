package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object SplitSteps {
  @StepFunction("772912d6-ee6a-5228-ae7a-0127eb2dce37",
    "Selects a subset of fields from a DataFrame",
    "Creates a new DataFrame which is a subset of the provided DataFrame",
    "Pipeline",
    "Example")
  def selectFields(dataFrame: DataFrame, fieldNames: List[String]): DataFrame =
    dataFrame.select(fieldNames.map(dataFrame(_)) : _*)
}
