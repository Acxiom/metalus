[Documentation Home](readme.md)

# Introduction
The Metalus library was started as a way to build Spark applications at runtime without the need to write or compile
code. The library is written in Scala and provides binaries for different versions of Spark and Scala. Developers build
applications by providing a JSON configuration file which gets loaded and executed by the 
[metalus core](../metalus-core/readme.md) library. 

## [Application Configuration](applications.md)
There are several methods for starting a metalus application, but the easiest to use is the application framework using
an application configuration. The application configuration JSON file provides information needed to run the application. 
Any application parameters provided to the _spark-submit_ command will automatically be added to the _globals_ and made 
available at runtime.

## [Step Libraries](step-libraries.md)
Step libraries contain the scala functions, pipeline configurations and custom driver classes which may be called at 
runtime using the provided configuration. 

### [Steps](steps.md)
Steps are scala object functions that are executed at runtime by metalus cores. Each function should be a small reusable
unit of work that stand alone and define requirements through parameters. Developers may [annotate](step-annotations.md) 
the functions or separately publish step templates which can be used when building applications. 

### [Pipelines](pipelines.md)
Pipeline [configurations written in JSON](json-pipelines.md) may be included in a step library. Developers will need to
provide a directory in the jar using the path _/metadata/pipelines_. The pipeline will need to be a JSON file where the 
pipeline id is the name plus the _.json_ extension.

### Custom Classes
Developers have the option to extend provided functionality such as drivers, driver setup, file managers and other classes.
These can be added to a step library and made available at runtime.

## [Self Contained Jar](metalus-application.md)
Metalus provides a self contained jar that may be used when calling the _spark-submit_ command. This jar contains no step
libraries which provides the developer control over what is available at runtime using the _--jars_ parameter.

## Versions
### Metalus Core
Metalus core provides the base library required to run metalus applications and build new step libraries.

|Spark Version|Scala Version|Library|
|-------------|-------------|-------|
|2.3          |2.11         |[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-core_2.11-spark_2.3)|
|2.4          |2.11         |[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-core_2.11-spark_2.4)|
|2.4          |2.12         |[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-core_2.12-spark_2.4)|

## Metalus Common
Metalus common provides a step library for building basic applications.

|Spark Version|Scala Version|Library|
|-------------|-------------|-------|
|2.3          |2.11         |[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-common_2.11-spark_2.3)|
|2.4          |2.11         |[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-common_2.11-spark_2.4)|
|2.4          |2.12         |[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-common_2.12-spark_2.4)|

## Metalus AWS
Metalus AWS provides a step library for working with AWS technologies.

|Spark Version|Scala Version|Library|
|-------------|-------------|-------|
|2.3          |2.11         |[Maven 2.11 Spark 2.3 library](https://search.maven.org/search?q=a:metalus-aws_2.11-spark_2.3)|
|2.4          |2.11         |[Maven 2.11 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-aws_2.11-spark_2.4)|
|2.4          |2.12         |[Maven 2.12 Spark 2.4 library](https://search.maven.org/search?q=a:metalus-aws_2.12-spark_2.4)|
