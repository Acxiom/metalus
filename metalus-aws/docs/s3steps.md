[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# S3Steps
S3Steps provides steps that allow a reading a DataFrame and writing a DataFrame to an S3 bucket.

## Write to Path
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **path** - A S3 path where the data will be written. The bucket should be part of the path.
* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter

## Read From Path
This function will read a file from the provided path into a DataFrame. Full parameter descriptions are listed below:

* **path** - A S3 file path to read. The bucket should be part of the path.
* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader

## Read From Paths
This function will read from each of the provided paths into a DataFrame. Full parameter descriptions are listed below:

* **paths** - A list of S3 file paths to read. The bucket should be part of each path.
* **accessKeyId** - The API key to use when connecting.
* **secretAccessKey** - The API secret to use when connecting.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader

## Create FileManager
This function will create a FileManager implementation that is useful for interacting with the S3 file system.

* **region** - The AWS region to connect through.
* **bucket** - The S3 bucket being used.
* **accessKeyId** - The optional API key to use when connecting.
* **secretAccessKey** - The optional API secret to use when connecting.

## Create FileManager With Existing Client
This function will create a FileManager implementation that is useful for interacting with the S3 file system. This call
will use the provided client.

* **s3Client** - The existing AWS client to connect through.
* **bucket** - The S3 bucket being used.
