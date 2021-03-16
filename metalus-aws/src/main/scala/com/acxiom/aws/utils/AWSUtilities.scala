package com.acxiom.aws.utils

import com.acxiom.pipeline.CredentialProvider

object AWSUtilities {
  /**
    * Return the Api Key and Secret if credentials are provided.
    *
    * @param credentialProvider The CredentialProvider
    * @param credentialName     The name of the credential
    * @return A tuple with the Api Key and Secret options
    */
  def getAWSCredentials(credentialProvider: Option[CredentialProvider], credentialName: String = "AWSCredential"): (Option[String], Option[String]) = {
    if (credentialProvider.isDefined) {
      val awsCredential = credentialProvider.get
        .getNamedCredential(credentialName).asInstanceOf[Option[AWSCredential]]
      if (awsCredential.isDefined) {
        (awsCredential.get.awsAccessKey, awsCredential.get.awsAccessSecret)
      } else {
        (None, None)
      }
    } else {
      (None, None)
    }
  }
}
