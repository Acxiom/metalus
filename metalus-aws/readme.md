[Documentation Home](../docs/readme.md)

# Metalus AWS Step Library
The Metalus AWS is a step library with specific steps and utilities for working with AWS technologies. The jar 
is compiled against specific versions of the AWS Java SDK libraries based on the version of Spark. The scopes
column shows the different scopes that can be used when running the [DependencyManager](../docs/dependency-manager.md).
Only one of the scopes is needed.

|Spark|Library|Version|Scopes|
------|-------|-------|------|
|2.4|spark-streaming-kinesis-asl_2.11|2.4.6|extraction,stream
|2.4|amazon-kinesis-client|1.12.0|extraction,stream
|2.4|aws-java-sdk-secretsmanager|1.11.595|extraction,secretsmanager
|2.4|aws-java-sdk-s3|1.11.595|extraction,s3
|2.4|aws-java-sdk-core|1.11.595|extraction,sdk
|2.4|hadoop-aws|2.7.7|extraction,s3a
|3.0|spark-streaming-kinesis-asl_2.12|3.0.0|extraction,stream
|3.0|amazon-kinesis-client|1.12.0|extraction,stream
|3.0|aws-java-sdk-secretsmanager|1.11.655|extraction,secretsmanager
|3.0|aws-java-sdk-s3|1.11.655|extraction,s3
|3.0|aws-java-sdk-core|1.11.655|extraction,sdk
|3.0|hadoop-aws|2.7.7|extraction,s3a

## Step Classes
* [S3Steps](docs/s3steps.md)
* [KinesisSteps](docs/kinesissteps.md)

## Pipelines/Step Groups
* [LoadS3Data](docs/loads3data.md)
* [WriteDataFrameToS3](docs/writedataframetos3.md)

## Extensions
* [S3FileManager](docs/s3filemanager.md)
* [Kinesis Pipeline Driver](docs/kinesispipelinedriver.md)
* [AWS Secrets Manager Credential Provider](docs/awssecretsmanager-credentialprovider.md)
