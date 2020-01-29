[Documentation Home](readme.md)

# Getting Started
The easiest way to learn the metalus library is to run an existing example. The following instructions are taken from the
[application example](application-example.md). This example will load in an example order file, split each record into
different DataFrames containing product, customer, credit card and order data. The data is then written to a Mongo data
store (named _application_examples_) in different collections. The metalus-common and metalus-examples step libraries 
will be required as well as the self contained [metalus application](metalus-application.md) jar.

## Build Metalus
Below are the build commands required to build the Metalus libraries:

|Spark Version|Scala Version|Command|
|-------------|-------------|-------|
|2.3          |2.11         |mvn clean install|
|2.4          |2.11         |mvn -Dspark.compat.version=2.4 -Djson4s.version=3.5.3 -Dspark.version=2.4.3 clean install|
|2.4          |2.12         |mvn -Dspark.compat.version=2.4 -Djson4s.version=3.5.3 -Dspark.version=2.4.3 -Dscala.compat.version=2.12 -Dscala.version=2.12.8 clean install|


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

## Run the application
The application commands below provide the proper templates to run the example:

* _\<VERSION>_ - The current Metalus version being built
* _<jar_path>_ - The fully qualified path to the built jars
* _<data_location>_ - The fully qualified path to the example data

### Spark 2.3/Scala 2.11
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.3-<VERSION>.jar,metalus-examples_2.11-spark_2.3-<VERSION>.jar  \
<jar_path>/metalus-application_2.11-spark_2.3-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <data_location>/application-example.json \
--input_url <data_location>/orders.csv \
--input_format csv \
--input_separator , \
--mongoURI mongodb://localhost:27017/application_examples \
--logLevel DEBUG
```
### Spark 2.4/Scala 2.11
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.4-<VERSION>.jar,metalus-examples_2.11-spark_2.4-<VERSION>.jar  \
<jar_path>/metalus-application_2.11-spark_2.4-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <data_location>/application-example.json \
--input_url <data_location>/orders.csv \
--input_format csv \
--input_separator , \
--mongoURI mongodb://localhost:27017/application_examples \
--logLevel DEBUG
```
### Spark 2.4/Scala 2.12
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.12-spark_2.4-<VERSION>.jar,metalus-examples_2.12-spark_2.4-<VERSION>.jar  \
<jar_path>/metalus-application_2.12-spark_2.4-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <data_location>/application-example.json \
--input_url <data_location>/orders.csv \
--input_format csv \
--input_separator , \
--mongoURI mongodb://localhost:27017/application_examples \
--logLevel DEBUG
```
