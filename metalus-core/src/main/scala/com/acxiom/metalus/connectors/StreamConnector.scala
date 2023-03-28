package com.acxiom.metalus.connectors

trait StreamConnector extends Connector {
  def connectorType: String = "STREAM"
}
