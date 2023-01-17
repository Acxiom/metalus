package com.acxiom.pipeline

import org.scalatest.funspec.AnyFunSpec

class CredentialProviderTests extends AnyFunSpec {
  describe("CredentialProvider - Default Implementation") {
    it("Should load default AuthorizationCredential") {
      val parameters = Map[String, Any](
        "authorization.class" -> "com.acxiom.pipeline.api.BasicAuthorization",
        "authorization.username" -> "redonthehead",
        "authorization.password" -> "fred",
        "credential-parsers" -> ",,,com.acxiom.pipeline.DefaultCredentialParser")
      val provider = new DefaultCredentialProvider(parameters)
      val credential = provider.getNamedCredential("DefaultAuthorization")
      assert(credential.isDefined)
      assert(credential.get.isInstanceOf[AuthorizationCredential])
      assert(Option(credential.get.asInstanceOf[AuthorizationCredential].authorization).isDefined)
      assert(provider.getNamedCredential("NoCredential").isEmpty)
    }

    it("Should load AuthorizationCredential from credential-classes") {
      val parameters = Map[String, Any](
        "credential-classes" -> ",com.acxiom.pipeline.AuthorizationCredential",
        "authorization.username" -> "redonthehead",
        "authorization.password" -> "fred")
      val provider = new DefaultCredentialProvider(parameters)
      val credential = provider.getNamedCredential("DefaultAuthorization")
      assert(credential.isDefined)
      assert(provider.getNamedCredential("NoCredential").isEmpty)
    }

    it("Should not load any credentials") {
      val provider = new DefaultCredentialProvider(Map[String, Any]("credential-classes" -> ",,,"))
      assert(provider.getNamedCredential("DefaultAuthorization").isEmpty)
    }

    it("Should create a DefaultCredential") {
      val credential = DefaultCredential(Map[String, Any]("credentialName" -> "test", "credentialValue" -> "cred_value"))
      assert(credential.name == "test")
      assert(credential.value == "cred_value")
    }
  }
}
