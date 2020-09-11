|Branch|Build|Coverage|Quality|
-------|-----|---------|-------|
|Develop|[![Develop Build](https://travis-ci.com/Acxiom/metalus.svg?branch=develop)](https://travis-ci.com/Acxiom/metalus?branch=develop)|[![Develop Coverage](https://img.shields.io/coveralls/github/Acxiom/metalus/develop.svg)](https://coveralls.io/github/Acxiom/metalus?branch=develop)|[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Acxiom_spark-pipeline-driver-develop&metric=alert_status)](https://sonarcloud.io/dashboard?id=Acxiom_spark-pipeline-driver-develop)|
|Master|[![Master Status](https://travis-ci.com/Acxiom/metalus.svg?branch=master)](https://travis-ci.com/Acxiom/metalus?branch=master)|[![Master Coverage](https://img.shields.io/coveralls/github/Acxiom/metalus/master.svg)](https://coveralls.io/github/Acxiom/metalus?branch=master)|[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Acxiom_spark-pipeline-driver-release&metric=alert_status)](https://sonarcloud.io/dashboard?id=Acxiom_spark-pipeline-driver-release)|

# Metalus Pipeline Library
The Metalus library was created as a way to create Spark applications at runtime without the need to write or compile
code. The library is written in Scala and provides binaries for different version of Spark and Scala. Developers build
applications by providing a JSON configuration file which gets loaded and executed by the metalus core library. 

## [Documentation](docs/readme.md)
Documentation for this project may be found [here](docs/readme.md).

## [Contributing](docs/contributions.md)
Instructions for contributing to this project and instructions on building may be found [here](docs/contributions.md).

## Projects
There are several sub-projects:

### [Metalus Pipeline Core](metalus-core/readme.md)
This project contains the core library and is the minimum requirement for any application.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-core_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-core_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-core_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-core_2.12-spark_3.0)

### [Metalus Common Step Library](metalus-common/readme.md)
This step library contains steps that are considered generic enough to be used in any project.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-common_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-common_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-common_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-common_2.12-spark_3.0)

### [Metalus AWS Step Library](metalus-aws/readme.md)
This step library contains AWS specific components. The [Kinesis](https://aws.amazon.com/kinesis/) driver provides a 
basic implementation that gathers data and then initiates the Metalus Pipeline Core for processing of the incoming data.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-aws_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-aws_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-aws_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-aws_2.12-spark_3.0)

### [Metalus GCP Step Library](metalus-gcp/readme.md)
This step library contains GCP specific components. The [Pub/Sub](https://cloud.google.com/pubsub/docs/overview) driver provides a 
basic implementation that gathers data and then initiates the Metalus Pipeline Core for processing of the incoming data.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-gcp_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-gcp_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-gcp_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-gcp_2.12-spark_3.0)

### [Metalus Kafka Step Library](metalus-kafka/readme.md)
This step library contains GCP specific components. The [Kafka](https://kafka.apache.org/) driver provides a 
basic implementation that gathers data and then initiates the Metalus Pipeline Core for processing of the incoming data.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-kafka_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-kafka_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-kafka_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-kafka_2.12-spark_3.0)

### [Metalus Mongo Step Library](metalus-mongo/readme.md)
This step library adds support for working with Mongo.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-mongo_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-mongo_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-mongo_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-mongo_2.12-spark_3.0)

### [Metalus Pipeline Examples](metalus-examples/readme.md)
This project provides several examples to help demonstrate how to use the library.

### [Metalus Utilities](metalus-utils/readme.md)
This project provides utilities that help work with the project.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-utils_2.11-spark_2.3)

[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-utils_2.11-spark_2.4)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-utils_2.12-spark_2.4)

[Maven 2.12 Spark 3.0 library](https://search.maven.org/search?q=a:metalus-utils_2.12-spark_3.0)

### [Metalus Application](metalus-application/readme.md)
This project provides a single jar that can be used to run the application. Additional components provide jars that can be
added to the classpath.

## Examples
Examples of building pipelines can be found in the [metalus-examples](metalus-examples/readme.md) project.
