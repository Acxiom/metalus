|Branch|Build|Coverage|Quality|
-------|-----|---------|-------|
|Develop|[![Develop Build](https://travis-ci.com/Acxiom/metalus.svg?branch=develop)](https://travis-ci.com/Acxiom/metalus?branch=develop)|[![Develop Coverage](https://img.shields.io/coveralls/github/Acxiom/metalus/develop.svg)](https://coveralls.io/github/Acxiom/metalus?branch=develop)|[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Acxiom_spark-pipeline-driver-develop&metric=alert_status)](https://sonarcloud.io/dashboard?id=Acxiom_spark-pipeline-driver-develop)|
|Master|[![Master Status](https://travis-ci.com/Acxiom/metalus.svg?branch=master)](https://travis-ci.com/Acxiom/metalus?branch=master)|[![Master Coverage](https://img.shields.io/coveralls/github/Acxiom/metalus/master.svg)](https://coveralls.io/github/Acxiom/metalus?branch=master)|[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Acxiom_spark-pipeline-driver-release&metric=alert_status)](https://sonarcloud.io/dashboard?id=Acxiom_spark-pipeline-driver-release)|

# Metalus Pipeline Library
The Metalus library provides a set of components for constructing data processing applications that can be executed against
the [Spark](http://spark.apache.org) framework using external configuration scripts written in JSON. Reusable logic is
maintained in the form of [Steps](metalus-core/docs/steps.md) written in Scala. Pipelines execute the steps in a flow 
defined using JSON (these may be coded as well). Executions are used to execute one or more pipelines sequentially and 
provide a mechanism for expressing dependencies. [Applications](metalus-core/docs/application.md) define the the 
configuration required to run the executions, pipelines and steps as well as initial setup information.

## Projects
There are several sub-projects:

### [Metalus Pipeline Core](metalus-core/readme.md)
This project contains the core library and is the minimum requirement for any application.

[Maven 2.11 Spark 2.3library](https://search.maven.org/search?q=a:metalus-core_2.11-spark_2.3)

[Maven 2.12 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-core_2.12-spark_2.3)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-core_2.12-spark_2.4)

### [Metalus Common Pipeline Components](metalus-common/readme.md)
This component contains steps that are considered generic enough to be used in any project.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-common_2.11-spark_2.3)

[Maven 2.12 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-common_2.12-spark_2.3)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-common_2.12-spark_2.4)

### [Metalus AWS](metalus-aws/readme.md)
This component contains AWS specific components. The [Kinesis](https://aws.amazon.com/kinesis/) driver provides a basic 
implementation that gathers data and then initiates the Metalus Pipeline Core for processing of the incoming data.

[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-aws_2.11-spark_2.3)

[Maven 2.12 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-aws_2.12-spark_2.3)

[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-aws_2.12-spark_2.4)

### [Metalus Pipeline Examples](metalus-examples/readme.md)
This project provides several examples to help demonstrate how to use the library.

### [Metalus Utilities](metalus-utils/readme.md)
This project provides utilities that help work with the project.

### [Metalus Application](metalus-application/readme.md)
This project provides a single jar that can be used to run the application. Additional components provide jars that can be
added to the classpath.

## Examples
Examples of building pipelines can be found in the [pipeline-drivers-examples](metalus-examples/readme.md) project.

## Building
The project is built using [Apache Maven](http://maven.apache.org/).
To build the project using Scala 2.11 and Spark 2.3 run:

	mvn

To build the project using Scala 2.11 and Spark 2.4 run:

	mvn -Dspark.compat.version=2.4 -Djson4s.version=3.5.3 -Dspark.version=2.4.3

To build the project using Scala 2.12 and Spark 2.4 run:

	mvn -Dspark.compat.version=2.4 -Djson4s.version=3.5.3 -Dspark.version=2.4.3 -Dscala.compat.version=2.12 -Dscala.version=2.12.8


(This will clean, build, test and package the jars and generate documentation)

## Running tests
Tests are part of the main build.

## Contributing
* Start by forking the main GutHub [repository](https://github.com/Acxiom/metalus).
* Commit all changes to the develop branch.
* Create proper scaladoc comments for any new or changed functions.
* Provide a thorough unit test for the change.
* Provide any additional documentation required by individual projects.
