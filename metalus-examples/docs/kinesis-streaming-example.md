# Simple Streaming Example (Kinesis)
The [**SimpleKinesisDriverSetup**](../src/main/scala/com/acxiom/pipeline/SimpleKinesisDriverSetup.scala) is very similar 
to the [**SimpleDataDriverSetup**](../src/main/scala/com/acxiom/pipeline/SimpleDataDriverSetup.scala) example that is 
covered in detail on the main example page [here](../readme.md). The difference is the use of the 
*com.acxiom.pipeline.drivers.KinesisPipelineDriver* driver. This will monitor a Kinesis stream and execute the 
specified pipeline for every batch of records that comes through. The defined pipeline will count the number of records 
in each *DataFrame*, then write them out to disk, continually appending to the same file.

### Grouping Steps
One extra grouping step was added that simply returns the number of records in a streaming *DataFrame*.

### Running
The code may be run using the provided [application jar](../../metalus-application/readme.md) for the main jar and the 
metalus-common and metalus-examples jars provided to the *--jars* parameter.

### Run the spark-submit command for Spark 2.3:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.3-<VERSION>.jar,metalus-examples_2.11-spark_2.3-<VERSION>.jar,metalus-aws_2.11-spark_2.3-<VERSION>.jar  \
<jar_path>/metalus-application_2.11-spark_2.3-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.SimpleKinesisDriverSetup \
--appName <Application name> \
--streamName <Stream name> \
--endPointURL <Endpoint URL.  EG : kinesis.us-east-1.amazonaws.com> \
--regionName <Region.  EG : us-east-1> \
--awsAccessKey <AWS Access Key> \
--awsAccessSecret <AWS Access Secret> \
--duration <Integer duration to collect each frame (in seconds)> \
--output_url <location to write the JSON file>
--logLevel DEBUG
```

### Run the spark-submit command for Spark 2.4:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.4-<VERSION>.jar,metalus-examples_2.11-spark_2.4-<VERSION>.jar,metalus-aws_2.11-spark_2.3-<VERSION>.jar  \
<jar_path>/metalus-application_2.11-spark_2.4-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.SimpleKinesisDriverSetup \
--appName <Application name> \
--streamName <Stream name> \
--endPointURL <Endpoint URL.  EG : kinesis.us-east-1.amazonaws.com> \
--regionName <Region.  EG : us-east-1> \
--awsAccessKey <AWS Access Key> \
--awsAccessSecret <AWS Access Secret> \
--duration <Integer duration to collect each frame (in seconds)> \
--output_url <location to write the JSON file>
--logLevel DEBUG
```
