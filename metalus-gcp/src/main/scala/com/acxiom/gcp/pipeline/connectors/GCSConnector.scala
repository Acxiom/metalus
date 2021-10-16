package com.acxiom.gcp.pipeline.connectors

import com.acxiom.gcp.pipeline.GCPCredential
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.connectors.Connector

trait GCSConnector extends Connector {
  override protected def getCredential(pipelineContext: PipelineContext): Option[GCPCredential] = {
    super.getCredential(pipelineContext).asInstanceOf[Option[GCPCredential]]
  }
}
