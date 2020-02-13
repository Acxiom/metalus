[Documentation Home](readme.md)

# Contributing
The Metalus project welcomes contributions in any form such as bug fixes, new features, examples and documentation. Start
by reading the documentation and then joining the 
[Slack](https://join.slack.com/t/acxiom-metalus/shared_invite/enQtODY3OTU0ODE5NzUwLTc2Zjc0MzE2MjYzZjBmZjJkODQxODhhOTM4N2VmZjNhZGVlN2Q3N2QzNWU3ZTk4NWExNWM2YzZkYTVjNjNiNWQ)
discussions.

## Code
* Start by forking the main GutHub [repository](https://github.com/Acxiom/metalus).
* Commit all changes to the develop branch.
* Create proper scaladoc comments for any new or changed functions.
    * Step functions should have proper [annotations](step-annotations.md).
* Provide a thorough unit test for the change.
* Provide any additional documentation required by individual projects.
* Discuss contributions on [Slack](https://join.slack.com/t/acxiom-metalus/shared_invite/enQtODY3OTU0ODE5NzUwLTc2Zjc0MzE2MjYzZjBmZjJkODQxODhhOTM4N2VmZjNhZGVlN2Q3N2QzNWU3ZTk4NWExNWM2YzZkYTVjNjNiNWQ)

## New Step libraries
Three step libraries are provided by metalus: [metalus-common](../metalus-common/readme.md), [metalus-mongo](../metalus-mongo) 
and [metlaus-aws](../metalus-aws/readme.md). Contributions should be made directly to the existing step libraries unless 
it represents specific functionality like support for Azure, GCP, etc. Before starting, review the different projects to 
get familiar with the coding style and learn if the functionality already exists.

## Building
The project is built using [Apache Maven](http://maven.apache.org/).

To build the project using Scala 2.11 and Spark 2.3 run:

	mvn

To build the project using Scala 2.11 and Spark 2.4 run:

	mvn -P spark_2.4

To build the project using Scala 2.12 and Spark 2.4 run:

	mvn -P spark_2.4,scala_2.12


(This will clean, build, test and package the jars and generate documentation)

## Running tests
Tests are part of the main build.
