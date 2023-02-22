package com.acxiom.metalus.steps

import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.annotations.{StepFunction, StepObject, StepParameter, StepParameters, StepResults}

@StepObject
object CredentialSteps {
  @StepFunction("86c84fa3-ad45-4a49-ac05-92385b8e9572",
    "Get Credential",
    "This step provides access to credentials through the CredentialProvider",
    "Pipeline",
    "Credentials")
  @StepParameters(Map(
    "credentialName" -> StepParameter(None, Some(true), None, None, None, None, Some("The dataset containing CSV strings"))))
  @StepResults(primaryType = "com.acxiom.pipeline.Credential",
    secondaryTypes = None)
  def getCredential(credentialName: String, pipelineContext: PipelineContext): Option[Credential] = {
    if (pipelineContext.credentialProvider.isDefined) {
      pipelineContext.credentialProvider.get.getNamedCredential(credentialName)
    } else {
      None
    }
  }
}