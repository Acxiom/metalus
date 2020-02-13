[Documentation Home](readme.md)

# Getting Started
The easiest way to learn the metalus library is to run an existing example. The following instructions are taken from the
[application example](application-example.md). This example will load an example order file, split each record into
different DataFrames containing product, customer, credit card and order data. The data is then written to a Mongo data
store (named _application_examples_) in different collections. The [metalus-common](../metalus-common/readme.md), 
[metalus-mongo](../metalus-mongo) and [metalus-examples](../metalus-examples/readme.md) [step libraries](step-libraries.md) 
will be required as well as the self contained [metalus application](metalus-application.md) jar.

## Build Metalus
Below are the basic build commands required to build the Metalus libraries:

|Spark Version|Scala Version|Command|
|-------------|-------------|-------|
|2.3          |2.11         |mvn clean install|
|2.4          |2.11         |mvn -P spark_2.4 clean install|
|2.4          |2.12         |mvn -P spark_2.4,scala_2.12 clean install|


## Mongo Setup
This example requires a local Mongo instance to be running. A free version may be downloaded 
[here](https://www.mongodb.com/download-center/community).

## Spark Setup
Download the desired version of [Spark](http://spark.apache.org/downloads.html) and unpack. Either the 2.3 or 2.4 versions
are supported.

Start the spark server from the unpacked directory with the following commands:

```shell script
cd <SPARK_INSTALL_DIR>
export SPARK_LOCAL_IP=127.0.0.1
sbin/start-master.sh -h localhost -p 7077
sbin/start-slave.sh localhost:7077 -h localhost
```

Stopping Spark requires the following commands:

```shell script
cd <SPARK_INSTALL_DIR>
sbin/stop-slave.sh
sbin/stop-master.sh
```

## Run the application
Commands to run this application are available [here](application-example.md#Running)
