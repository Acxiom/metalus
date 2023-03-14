package com.acxiom.metalus.aws.utils

import com.acxiom.metalus.CredentialProvider
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsSessionCredentials, ProfileCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.client.builder.{AwsClientBuilder, AwsSyncClientBuilder}
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

object AWSUtilities {
  private val MAX_SESSION_LENGTH = 64

  /**
   * Builds an ARN using the information provided.
   *
   * @param accountId The AWS account.
   * @param role      The role for the ARN
   * @param partition Defaults to aws.
   * @return A role ARN
   */
  def buildRoleARN(accountId: String, role: String, partition: Option[String]): String = {
    s"arn:${partition.getOrElse("aws")}:iam::$accountId:role/$role"
  }

  /**
   * Performs an assume role operation and returns a key, secret and session token.
   *
   * @param accountId
   * @param role
   * @param partition
   * @param session
   * @param externalId
   * @param duration
   * @return A key, secret and session token
   */
  def assumeRole(accountId: String,
                 role: String,
                 partition: Option[String] = None,
                 session: Option[String] = None,
                 externalId: Option[String] = None,
                 duration: Option[Integer] = None): AWSCredentialInfo = {

    val stsClient = StsClient.builder()
      .credentialsProvider(ProfileCredentialsProvider.create())
      .build()

    val arn = buildRoleARN(accountId, role, partition)
    val sessionName = session.getOrElse(s"${accountId}_$role")

    val roleRequest = AssumeRoleRequest.builder()
      .roleArn(arn)
      .roleSessionName(sessionName.take(MAX_SESSION_LENGTH))
    val withExternalId = externalId.filter(_.trim.nonEmpty).map(roleRequest.externalId).getOrElse(roleRequest)
    val withDuration = duration.map(withExternalId.durationSeconds).getOrElse(withExternalId)
    val credentials = stsClient.assumeRole(withDuration.build()).credentials()
    AWSCredentialInfo(Some(credentials.accessKeyId()), Some(credentials.secretAccessKey()), Some(credentials.sessionToken()))
  }

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

  /**
   * Finds the named credential in the CredentialProvider.
   *
   * @param credentialProvider The CredentialProvider to search.
   * @param credentialName The name of the credential to locate.
   * @return A credential or None.
   */
  def getAWSCredential(credentialProvider: Option[CredentialProvider], credentialName: String = "AWSCredential"): Option[AWSCredential] = {
    credentialProvider.flatMap(cp => cp.getNamedCredential(credentialName).map(_.asInstanceOf[AWSCredential]))
  }

  def setupCredentialProvider(builder: Any, credential: Option[AWSCredential] = None): Any = {
    builder match {
      case clientBuilder: AwsClientBuilder[_, _] if credential.isDefined =>
        val info = credential.get.buildAWSCredentialInfo
        if (info.sessionToken.isDefined) {
          clientBuilder.credentialsProvider(
            StaticCredentialsProvider.create(AwsSessionCredentials.create(info.accessKeyId.getOrElse(""),
            info.secretAccessKey.getOrElse(""),
            info.sessionToken.getOrElse(""))))
        } else if (info.accessKeyId.isDefined && info.secretAccessKey.isDefined) {
          clientBuilder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(info.accessKeyId.get, info.secretAccessKey.get)))
        }
      case _ => builder
    }
  }
}
