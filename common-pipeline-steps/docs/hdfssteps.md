# HDFSSteps
This step object provides a way to read from and write to HDFS. There are two step functions provided:

## Write DataFrame
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to HDFS.
* **path** - A HDFS path to write to.
* **format** - A format recognized by DataFrameWriter. The Default value is "_parquet_".
* **properties** - Optional Map[String,String] of properties for the given format
* **saveMode** - The Writing behavior. Valid values: (Append, Overwrite, ErrorIfExists, Ignore). 
The default value is "_Overwrite_".

## Read From HDFS
This function will read a file from the provided path into a DataFrame. Full parameter descriptions are listed below:

* **path** - A HDFS file path to read.
* **format** - A format recognized by DataFrameWriter. The Default value is "_parquet_".
* **properties** - Optional Map[String,String] of properties for the given format.
