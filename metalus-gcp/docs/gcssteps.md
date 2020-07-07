[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# GCSSteps
GCSSteps provides steps that allow a reading a DataFrame and writing a DataFrame to a GCS bucket.

## Write to Path
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to GCS.
* **path** - A GCS path where the data will be written. The bucket should be part of the path.
* **credentials** - The credential map to use or none to use the system provided credentials.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter

## Read From Path
This function will read a file from the provided path into a DataFrame. Full parameter descriptions are listed below:

* **path** - A GCS file path to read. The bucket should be part of the path.
* **credentials** - The credential map to use or none to use the system provided credentials.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader

## Read From Paths
This function will read from each of the provided paths into a DataFrame. Full parameter descriptions are listed below:

* **paths** - A list of GCS file paths to read. The bucket should be part of each path.
* **credentials** - The credential map to use or none to use the system provided credentials.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader

## Create FileManager
This function will create a FileManager implementation that is useful for interacting with the the GCS file system.

* **credentials** - The credential map to use or none to use the system provided credentials.
* **projectId** - The GCP project that owns the bucket.
* **bucket** - The GCS bucket being used.
