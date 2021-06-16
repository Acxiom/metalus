[Documentation Home](../docs/readme.md)

# Metalus Delta Lake Step Library
The Metalus Delta Lake is a step library with specific steps and utilities for working with the Detla Lake datasource. The jar 
is compiled against specific versions of the delta-core library based on the version of Spark. The scopes
column shows the different scopes that can be used when running the [DependencyManager](../docs/dependency-manager.md).
Only one of the scopes is needed.

|Spark|Library|Version|Scopes|
|------|-------|-------|------|
|2.4|delta-core_2.11|0.6.1|extraction,deltalake|
|2.4|delta-core_2.12|0.6.1|extraction,deltalake|
|3.0|delta-core_2.12|0.8.0|extraction,deltalake|
|3.1|delta-core_2.12|1.0.0|extraction,deltalake|

## Step Classes
* [DeltaLakeSteps](docs/deltalakesteps.md)
