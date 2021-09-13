package com.acxiom.aws.pipeline.connectors

import com.acxiom.aws.utils.AWSCredential
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.connectors.DataConnector

trait AWSDataConnector extends DataConnector{
  protected def getCredential(pipelineContext: PipelineContext): Option[AWSCredential] = {
    (if (credentialName.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName.get)
    } else {
      credential
    }).asInstanceOf[Option[AWSCredential]]
  }
}
