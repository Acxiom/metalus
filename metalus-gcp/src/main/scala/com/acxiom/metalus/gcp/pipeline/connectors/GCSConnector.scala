package com.acxiom.metalus.gcp.pipeline.connectors

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.connectors.Connector
import com.acxiom.metalus.gcp.pipeline.GCPCredential

trait GCSConnector extends Connector {
  override protected def getCredential(pipelineContext: PipelineContext): Option[GCPCredential] = {
    super.getCredential(pipelineContext).asInstanceOf[Option[GCPCredential]]
  }
}
