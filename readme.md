# Spark Pipeline Driver
This project aims to make writing [Spark](http://spark.apache.org) applications easier by abstracting the effort to assemble the driver into
reusable steps and pipelines.

## Building
The project is built using [Apache Maven](http://maven.apache.org/).
To build the project run:

	mvn

(This will clean, build, test and package the jars and generate documentation)

## Running tests
Tests are part of the main build.

## Spark Versions
This project is built using a specific Spark version and Scala version.

## Components
The project is made up of two main components:

* Spark Pipeline Engine
* Streaming Pipeline Drivers

### Spark Pipeline Engine
This component contains all of the functionality required to build processing pipelines. Processing pipelines contain a
series of PipelineSteps which are executed in order, conditionally or as branching logic. Multiple pipelines may be
chained together in a single Spark session with objects from pipelines being made available to subsequent pipelines.

### Streaming Pipeline Drivers
This component contains drivers classes that connect to various streaming technologies like [Kafka](http://kafka.apache.org/) and [Kinesis](https://aws.amazon.com/kinesis/). Each
class provides a basic implementation that gathers data and then initiates the Spark Pipeline Engine Component for
processing of the incoming data.