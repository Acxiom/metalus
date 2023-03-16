package com.acxiom.metalus.aws.spark

import com.acxiom.metalus.aws.utils.AWSCredential

package object connectors {

  implicit class AWSCredentialImplicit(credential: AWSCredential) {
    def toSparkOptions: Map[String, String] = credential.awsRoleARN.map{ arn =>
      Map(
        "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
        "fs.s3a.assumed.role.arn" -> arn
      ) ++ credential.sessionName.map("fs.s3a.assumed.role.session.name" -> _) ++
        credential.duration.map("fs.s3a.assumed.role.session.duration" -> _)
    } getOrElse {
      Map(
        "fs.s3a.access.key" -> credential.awsAccessKey.mkString,
        "fs.s3a.secret.key" -> credential.awsAccessSecret.mkString
      )
    }
  }

}
