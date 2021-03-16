package com.acxiom.pipeline

import org.scalatest.FunSpec

class CredentialProviderTests extends FunSpec {
  describe("CredentialProvider - Default Implementation") {
    it("Should load default AuthorizationCredential") {
      val parameters = Map[String, Any](
        "authorization.class" -> "com.acxiom.pipeline.api.BasicAuthorization",
        "authorization.username" -> "redonthehead",
        "authorization.password" -> "fred")
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
  }
}
