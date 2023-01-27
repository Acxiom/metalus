package com.acxiom.metalus.steps

import com.acxiom.metalus._
import com.acxiom.metalus.context.ContextManager
import org.scalatest.funspec.AnyFunSpec

class CredentialStepsTests extends AnyFunSpec {
  describe("CredentialSteps") {
    it("Should retrieve a credential") {
      val credentialProvider = new DefaultCredentialProvider(Map[String, Any](
        "credential-classes" -> "com.acxiom.metalus.DefaultCredential",
        "credentialName" -> "bob",
        "credentialValue" -> "bob's credential"))
      val pipelineContext = PipelineContext(Some(Map[String, Any]()), List(),
        Some(List("com.acxiom.metalus.steps")), PipelineStepMapper(),
        Some(DefaultPipelineListener()), List(), credentialProvider = Some(credentialProvider),
        contextManager = new ContextManager(Map(), Map()))
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
