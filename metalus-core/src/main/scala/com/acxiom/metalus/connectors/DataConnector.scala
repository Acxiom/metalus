package com.acxiom.metalus.connectors

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.sql.DataReference

trait DataConnector extends Connector {
  def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): DataReference[_]
}
