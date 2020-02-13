# Metalus Pipeline Examples
This project contains examples related to using the different features of the framework. All examples present some form
of coding exercise as a way of teaching the basic requirements, however using the *Application* framework and 
*metalus-common* project, it may be entirely possible to write a complete application without the need to build a jar
or create new steps.

* [Application Example](../docs/application-example.md) - This example uses the *Application* framework to build the 
example. It is similar to the [Execution Plan Example](docs/pipeline-execution-plan-example.md), but does not
require building the *DriverSetup*.
* [Application Example from Metadata in Jar](docs/application-jarmeta-example.md) - This example uses the *Application*
framework pointing to a pipeline stored in the metalus-common metadata. It uses a step-group to load data from an SFTP
site into and HDFS "bronze" datastore (parquet).
* [Basic ETL Example](docs/simple-batch-example.md) - This example demonstrates reading in data from a file, processing 
the data and then writing the processed data back to disk. 
* [Execution Plan Example](docs/pipeline-execution-plan-example.md) - This example explores the execution plan functionality.
* [Kinesis Streaming Example](docs/kinesis-streaming-example.md) - This example demonstrates how to process data from Kinesis.
