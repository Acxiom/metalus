# JDBCSteps
This step object provides a way to read and write via JDBC. A case class named JDBCStepOptions is provided 
that accepts a url, a table name, and a Map[String, String] of properties.
There are six step functions provided:

##Read With JDBCOptions
Given a JDBCOptions object, this step will read a table into a DataFrame. Full parameter descriptions are listed below:

* **jDBCOptions** - A JDBCOptions object used to connect.

## Read With StepOptions
This step uses a JDBCDataFrameReaderOptions, which should be used if more control on the underlying DataFrameReader
object is desired. Using the provided options, this step will read a table over jdbc into a DataFrame.
Full parameter descriptions are listed below:

* **jDBCStepOptions** - A JDBCDataFrameReaderOptions object used to connect.

## Read With Properties
This step allows for a list of predicates to be provided,
allowing for more control over the partitioning behavior when reading the table.
Given a url and table name, this step will read a table into a DataFrame. Full parameter descriptions are listed below:

* **url** - A valid jdbc url.
* **table** - A table name or sub query
* **predicates** - An optional Array[String] of predicates used for partitioning
* **connectionProperties** - Optional Map[String,String] of properties for the given format.

## Write With JDBCOptions
Given a JDBCOptions object and a DataFrame, this step will write to a table via JDBC. 
Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written via JDBC.
* **jDBCOptions** - A JDBCOptions object used to connect.
* **saveMode** - The Writing behavior. Valid values: (Append, Overwrite, ErrorIfExists, Ignore). 
The default value is "_Overwrite_".

## Write With StepOptions
This step uses a JDBCDataFrameWriterOptions, which should be used if more control on the underlying DataFrameWriter
object is desired. Using the provided options, this step will write to a table via JDBC.
Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written via JDBC.
* **jDBCStepOptions** - A JDBCDataFrameWriterOptions object used to connect.

## Write With Properties
Given a url and table name, this step will write to a table via JDBC. 
Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written via JDBC.
* **url** - A valid jdbc url.
* **table** - A table name or sub query
* **connectionProperties** - Optional Map[String,String] of properties for the given format.
* **saveMode** - The Writing behavior. Valid values: (Append, Overwrite, ErrorIfExists, Ignore). 
The default value is "_Overwrite_".