# Metalus Pipeline Library
The Metalus pipeline library provides a convenient way to build applications using JSON. Additional libraries are provided
for working in different cloud environments and with different versions of Spark.

## [Documentation](docs/readme.md)
Documentation for this project may be found [here](docs/readme.md).

## [Contributing](docs/contributions.md)
Instructions for contributing to this project and instructions on building may be found [here](docs/contributions.md).

## Libraries
There are several libraries provided:

### [Metalus Core Library](metalus-core/readme.md)
The core library is the minimum required to start building applications that can run on any machine. This library provides
the core mapping, data reference, application, pipeline and extension mechanisms. There are versions available for Scala
2.12 and 2.13. This library may be integrated into any Maven project with the following dependency:

```xml
        <dependency>
            <groupId>com.acxiom</groupId>
            <artifactId>metalus-core_${scala.compat.version}</artifactId>
            <version>${version}</version>
            <scope>provided</scope>
        </dependency>
```

### [Metalus AWS Library](metalus-aws/readme.md)
The AWS library is built on top of the core library and provides components and steps for working with AWS services such
as Kinesis, S3, Lambda and Secrets Manager. There are builds for Scala 2.12 and 2.13. This library may be integrated into
any Maven project using the core dependency and this additional dependency:

```xml
        <dependency>
            <groupId>com.acxiom</groupId>
            <artifactId>metalus-aws_${scala.compat.version}</artifactId>
            <version>${version}</version>
            <scope>provided</scope>
        </dependency>
```

### [Metalus GCP Library](metalus-gcp/readme.md)
The GCP library is built on top of the core library and provides components and steps for working with GCP services such
as Pub/Sub, GCS, Cloud Functions and Secrets Manager. There are builds for Scala 2.12 and 2.13. This library may be
integrated into any Maven project using the core dependency and this additional dependency:

```xml
        <dependency>
            <groupId>com.acxiom</groupId>
            <artifactId>metalus-gcp_${scala.compat.version}</artifactId>
            <version>${version}</version>
            <scope>provided</scope>
        </dependency>
```

### [Metalus Spark Library](metalus-spark/readme.md)
The Spark library is built on top of the core library and provides integration with Apache Spark. A special `Context`
is provided that allows setting up a `SparkSession` as wells as steps for preforming runtime configurations and steps
/components for working with Structured Streaming. There are builds for Scala 2.12 and 2.13 and Spark versions 3.1, 3.2
and 3.3. This library may be integrated into any Maven project using the core dependency and this additional dependency:

```xml
        <dependency>
            <groupId>com.acxiom</groupId>
            <artifactId>metalus-spark_${scala.compat.version}-spark_${spark.compat.version}</artifactId>
            <version>${version}</version>
            <scope>provided</scope>
        </dependency>
```

### [Metalus AWS Spark Library](metalus-aws-spark/readme.md)
The AWS Spark library is built on top of the core, AWS and Spark libraries and provides components and steps for working 
with AWS services using Spark as the execution engine. There are builds for Scala 2.12 and 2.13 and Spark versions 3.1,
3.2 and 3.3. This library may be integrated into any Maven project using the core, aws and spark dependencies and this
additional dependency:

```xml
        <dependency>
            <groupId>com.acxiom</groupId>
            <artifactId>metalus-aws-spark_${scala.compat.version}-spark_${spark.compat.version}</artifactId>
            <version>${version}</version>
            <scope>provided</scope>
        </dependency>
```

### [Metalus Utilities](metalus-utils/readme.md)
The Metalus Utilities project provides tools for extracting step, pipeline and application metadata from the standard and
extended libraries. The dependency resolution tool provides the ability to build a classpath from one or more of the libraries.
