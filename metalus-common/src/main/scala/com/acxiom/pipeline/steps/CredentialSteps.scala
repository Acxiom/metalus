package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.{Credential, PipelineContext}

@StepObject
object CredentialSteps {
  @StepFunction("86c84fa3-ad45-4a49-ac05-92385b8e9572",
    "Get Credential",
    "This step provides access to credentials through the CredentialProvider",
    "Pipeline",
    "Credentials")
  @StepParameters(Map(
    "credentialName" -> StepParameter(None, Some(true), None, None, None, None, Some("The dataset containing CSV strings"))))
  def getCredential(credentialName: String, pipelineContext: PipelineContext): Option[Credential] = {
    if (pipelineContext.credentialProvider.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName)
    } else {
      None
    }
  }
}
