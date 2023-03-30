package com.acxiom.metalus.steps

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.annotations.StepFunction
import com.acxiom.metalus.connectors.{DataConnector, InMemoryDataConnector}
import com.acxiom.metalus.sql.{DataReference, InMemoryDataReference}

object DataReferenceSteps {

  @StepFunction("6847964a-c1b6-41cb-ad5a-fb192e12b3d5",
    "Create Data Reference",
    "Create a DataReference object given a DataConnector",
    "Pipeline",
    "Data Reference")
  def createDataReference(connector: DataConnector,
                          properties: Option[Map[String, Any]], pipelineContext: PipelineContext): DataReference[_] =
    connector.createDataReference(properties, pipelineContext)


  @StepFunction("91bd5f24-ac28-4695-8a3c-f1d46f76031b",
    "Create InMemory Data Reference",
    "Create an InMemoryDataReference object given an InMemoryDataConnector",
    "Pipeline",
    "Data Reference")
  def createInMemoryDataReference(connector: InMemoryDataConnector,
                                  properties: Option[Map[String, Any]], pipelineContext: PipelineContext): InMemoryDataReference =
    connector.createDataReference(properties, pipelineContext).asInstanceOf[InMemoryDataReference]
}
