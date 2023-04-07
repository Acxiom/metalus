package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepObject}
import com.acxiom.metalus.connectors.{Connector, ConnectorProvider, DataConnector, FileConnector}

@StepObject
object ConnectorSteps {
  @StepFunction("352abe61-6852-4844-9435-2ca427bcd45b",
    "Get Connector",
    "This step provides access to connectors through the ConnectorProvider",
    "Pipeline",
    "Connectors")
  def getConnector(name: String,
                   uri: String,
                   connectorType: Option[String] = None,
                   parameters: Option[Map[String, Any]] = None): Option[Connector] =
    ConnectorProvider.getConnector(name, uri, connectorType, parameters)

  @StepFunction("c4c03542-e70b-4b3d-ac6a-ce6065a4fbf7",
    "Get File Connector",
    "This step provides access to file connectors through the ConnectorProvider",
    "Pipeline",
    "Connectors")
  def getFileConnector(name: String,
                       uri: String,
                       parameters: Option[Map[String, Any]] = None): Option[FileConnector] =
    ConnectorProvider.getConnector(name, uri, Some("FILE"), parameters).map(_.asInstanceOf[FileConnector])

  @StepFunction("543ee9e3-0a73-49d9-993b-5531b3e4ef1b",
    "Get Data Connector",
    "This step provides access to data connectors through the ConnectorProvider",
    "Pipeline",
    "Connectors")
  def getDataConnector(name: String,
                       uri: String,
                       parameters: Option[Map[String, Any]] = None): Option[DataConnector] =
    ConnectorProvider.getConnector(name, uri, Some("DATA"), parameters).map(_.asInstanceOf[DataConnector])
}
