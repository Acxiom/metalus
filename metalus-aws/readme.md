[Documentation Home](../docs/readme.md)

# Metalus AWS Step Library
The Metalus AWS is a step library with specific steps and utilities for working with AWS technologies.

## KinesisPipelineDriver
This driver provides basic support for streaming data from [Kinesis](https://aws.amazon.com/kinesis/) streams. As data
is consumed, the RDD will be converted into a DataFrame with three columns:

* **key** - the partitionKey
* **value** - the data
* **topic** - The appName

### Command line Parameters
*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
* **appName** - The name of this app to use when check pointing Kinesis sequence numbers.
* **streamName** - The Kinesis stream where data will be pulled
* **endPointURL** - The Kinesis URL where to connect the app.
* **regionName** - A valid AWS region
* **awsAccessKey** - The AWS access key used to connect
* **awsAccessSecret** - The AWS access secret used to connect

*Optional Parameters:*
* **consumerStreams** - [number] The number of streams to create. Defaults to the number of shards defined for the stream.
* **duration-type** - [minutes | **seconds**] Corresponds to the *duration* parameter.
* **duration** - [number] How long the driver should wait before processing the next batch of data. Default is 10 seconds.
* **terminationPeriod** - [number] The number of ms the system should run and then shut down. 

## S3FileManager
Provides a [FileManager](../metalus-core/docs/filemanager.md) implementation that works with the S3 file system.  Full
parameter descriptions are listed below:

* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **region** - The AWS region to connect through.
* **bucket** - The S3 bucket being used.

## S3Steps
S3Steps provides steps that allow a reading a DataFrame and writing a DataFrame to an S3 bucket.

### Write to Path
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **path** - A S3 path where the data will be written. The bucket should be part of the path.
* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter

### Read From Path
This function will read a file from the provided path into a DataFrame. Full parameter descriptions are listed below:

* **path** - A S3 file path to read. The bucket should be part of the path.
* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader

### Read From Paths
This function will read from each of the provided paths into a DataFrame. Full parameter descriptions are listed below:

* **paths** - A list of S3 file paths to read. The bucket should be part of each path.
* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader

### Create FileManager
This function will create a FileManager implementation that is useful for interacting with the the S3 file system.

* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **region** - The AWS region to connect through.
* **bucket** - The S3 bucket being used.
