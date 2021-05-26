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
    * Step functions must have proper [annotations](step-annotations.md).
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

To build the project using Scala 2.11 and Spark 2.4 run:

	mvn

To build the project using Scala 2.12 and Spark 3.0 run:

	mvn -P spark_3.0


(This will clean, build, test and package the jars and generate documentation)

## Integration Tests
The integration test harness will build the code against each specific version combination. Four versions of Metalus
are tested:

* 2.4
* 2.4_2.12 (does not run manual tests)
* 3.0
* 3.1
* all (default)

The command to execute these tests is **manual-tests.sh** and has two optional parameters:
* --save-metadata: Save the generated metadata to the _metadata_templates_ directory.
* --version: Specify one of the versions to execute. Default is 'all'.

The tests consist of 3 stages:

### Compile and Unit test
The code will be built against a specific version and then unit tests will be executed.
### Spark Server test
This test will run the code built in the previous step against a Spark and Mongo server. Mongo must be 
installed and on the path. The test will look to see if the proper version of the Spark server is available
before downloading and extracting. Once the server and forked Mongo process are running, the test will run
a basic application to load data from a provided file, do some processing and then write the output to Mongo
where it will be validated.
### Metadata Extractor test
The [Metadata Extractor](metadata-extractor.md) will run against each of the [step libraries](step-libraries.md) and compare
the output against a known good set. Any changes/additions to the step library will require the known set to be updated.

### Example Commands
#### Base:
```shell
./manual-tests.sh
```
#### Save Metadata
This command will run all versions and save the metadata generated in the [Metadata Extarctor test](#metadata-extractor-test)
to the _metadata_templates_ directory.
```shell
./manual-tests.sh --save-metadata true
```
#### Run a single verson
```shell
./manual-tests.sh --version 3.1
```
