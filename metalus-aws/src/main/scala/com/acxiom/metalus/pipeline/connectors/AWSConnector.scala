package com.acxiom.metalus.pipeline.connectors

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.connectors.Connector
import com.acxiom.metalus.utils.AWSCredential

trait AWSConnector extends Connector {
  override protected def getCredential(pipelineContext: PipelineContext): Option[AWSCredential] = {
    super.getCredential(pipelineContext).asInstanceOf[Option[AWSCredential]]
  }
}
