# Common Pipeline Steps
The goal of this project is to provide a set of common steps to help application developers focus on building applications
and custom steps. Any step accepted into this project must be annotated, provide a unit test,
provide api documentation for all functions and provide documentation with additional usage information.

## Steps
Here is a list of steps provided:

* [HDFSSteps](docs/hdfssteps.md)
* [JavascriptSteps](docs/javascriptsteps.md)
* [TransformationSteps](docs/transformationsteps.md)
* [JDBCSteps](docs/jdbcsteps.md)
* [QuerySteps](docs/querysteps.md)
* [ScalaSteps](docs/scalascriptsteps.md)

## Application Jar
In order to make running jobs easier, an uber jar is provided that can be deployed. This jar contains all of the steps
in this project as well as the library code. The jar will be part of the release or can be built locally. The jar name
follows the pattern: **common-steps-application_${scala.compat.version}-${version}.jar**
