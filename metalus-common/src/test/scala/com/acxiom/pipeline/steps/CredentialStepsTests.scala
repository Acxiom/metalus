package com.acxiom.pipeline.steps

import com.acxiom.pipeline._
import org.scalatest.FunSpec

class CredentialStepsTests extends FunSpec {
  describe("CredentialSteps") {
    it("Should retrieve a credential") {
      val credentialProvider = new DefaultCredentialProvider(Map[String, Any](
        "credential-classes" -> "com.acxiom.pipeline.DefaultCredential",
        "credentialName" -> "bob",
        "credentialValue" -> "bob's credential"))
      val pipelineContext = PipelineContext(None, None, Some(Map[String, Any]()),
        PipelineSecurityManager(),
        PipelineParameters(List()),
        Some(List("com.acxiom.pipeline.steps")),
        PipelineStepMapper(),
        Some(DefaultPipelineListener()),
        None,
        credentialProvider = Some(credentialProvider))
      val credential = CredentialSteps.getCredential("bob", pipelineContext)
      assert(credential.isDefined)
      assert(credential.get.name == "bob")
      assert(credential.get.isInstanceOf[DefaultCredential])
      assert(credential.get.asInstanceOf[DefaultCredential].value == "bob's credential")
      val badCredential = CredentialSteps.getCredential("no credential", pipelineContext.copy(credentialProvider = None))
      assert(badCredential.isEmpty)
    }
  }
}
