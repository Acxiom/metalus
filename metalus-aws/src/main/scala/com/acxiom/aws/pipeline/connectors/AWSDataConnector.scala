package com.acxiom.aws.pipeline.connectors

import com.acxiom.aws.utils.AWSCredential
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.connectors.DataConnector

trait AWSDataConnector extends DataConnector{
  override protected def getCredential(pipelineContext: PipelineContext): Option[AWSCredential] = {
    super.getCredential(pipelineContext).asInstanceOf[Option[AWSCredential]]
  }
}
