[Documentation Home](../../docs/readme.md) | [AWS Spark Home](../readme.md)

# AWSSparkOptionsProvider
This implements the SparkOptionsProvider trait to build options when any AWSCredentials are passed to a spark data
connector. The following options are built and combined to the dataFrameReader/Writer options:

| Option                               | Value                                                       |
|--------------------------------------|-------------------------------------------------------------|
| fs.s3a.aws.credentials.provider      | org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider |
 | fs.s3a.assumed.role.arn              | <credential.awsRoleARN>                                     |
 | fs.s3a.assumed.role.session.name     | <credential.sessionName>                                    |
 | fs.s3a.assumed.role.session.duration | <credential.duration>                                       |
 | fs.s3a.access.key                    | <credential.awsAccessKey>                                   |
 | fs.s3a.secret.key                    | <credential.awsAccessSecret>                                |
