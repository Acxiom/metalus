package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.connectors.DataConnector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

@StepObject
object DataConnectorSteps {

  @StepFunction("836aab38-1140-4606-ab73-5b6744f0e7e7",
    "Load",
    "This step will create a DataFrame using the given DataConnector",
    "Pipeline",
    "Connectors")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to write")),
    "connector" -> StepParameter(None, Some(true), None, None, None, None, Some("The data connector to use when writing")),
    "source" -> StepParameter(None, Some(false), None, None, None, None, Some("The source path to load data"))))
  def loadDataFrame(connector: DataConnector,
                    source: Option[String],
                    pipelineContext: PipelineContext): DataFrame =
    connector.load(source, pipelineContext)

  @StepFunction("5608eba7-e9ff-48e6-af77-b5e810b99d89",
    "Write",
    "This step will write a DataFrame using the given DataConnector",
    "Pipeline",
    "Connectors")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to write")),
    "connector" -> StepParameter(None, Some(true), None, None, None, None, Some("The data connector to use when writing")),
    "destination" -> StepParameter(None, Some(false), None, None, None, None, Some("The destination path to write data"))))
  def writeDataFrame(dataFrame: DataFrame,
                     connector: DataConnector,
                     destination: Option[String],
                     pipelineContext: PipelineContext): Option[StreamingQuery] =
    connector.write(dataFrame, destination, pipelineContext)
}
