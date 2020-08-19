[Documentation Home](../docs/readme.md)

# Metalus Core
The Metalus Core project provides the base functionality needed to build Spark applications. The core library provides the
building blocks required to create [applications](../docs/applications.md), [step libraries](../docs/step-libraries.md)
and [pipelines](../docs/pipelines.md). It is advised that developers use the application framework and provided step 
libraries when building Spark applications, however several extensions points have been provided to allow developers
the ability to construct custom step libraries and applications:

* [Pipeline Driver](../docs/pipeline-drivers.md) - This is the entry point into a Metalus application. A default 
implementation is provided and should meet all of the batch needs.
* [Driver Setup](../docs/pipeline-drivers.md#driversetup) - This is called by the _Pipeline Driver_ to build out the
[execution plan](../docs/executions.md) which will be executed. This is the most common extension when providing custom
behaviors not provided by the [application framework](../docs/applications.md). 
* [File Manager](../docs/filemanager.md) - The _FileManager_ class provides access to the underlying file system from 
[steps](../docs/steps.md). Implementations for local (used for development) and HDFS are provided by the core library.


There are several utility classes provided to make working with Metalus easier:

* [Driver Utils](../docs/driver-utils.md) - Provides functions for building Spark Conf objects, extracting command line parameters, validating 
required parameters and loading/parsing JSON into executable objects.
* [Execution Audits](../docs/executionaudits.md) - Provides a way to audit executions at runtime.
* [Http Rest Client](../docs/httprestclient.md) - Provides a basic rest client for communicating with rest APIs. Custom 
_Authorization_ implementations may be provided.
