package com.acxiom.metalus.aws.pipeline.connectors

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.aws.utils.AWSCredential
import com.acxiom.metalus.connectors.Connector

trait AWSConnector extends Connector {
  override protected def getCredential(pipelineContext: PipelineContext): Option[AWSCredential] = {
    super.getCredential(pipelineContext).asInstanceOf[Option[AWSCredential]]
  }
}
