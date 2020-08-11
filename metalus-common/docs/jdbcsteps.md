[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

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

## Get JDBC Connection
Given a url and map of properties, this step will return a jdbc connection. 
Full parameter descriptions are listed below:

* **url** - A valid jdbc url.
* **properties** - Optional Map[String,String] of connection properties.

## Execute Sql
Given a sql statement and jdbc connection, this step will execute the statement and return a List[Map[String, Any]] containing the results.
If the statement is an update, the results list will be empty. A namedResult, *count*, will also be provided,
 to represent the number of records returned/modified. 
Full parameter descriptions are listed below:

* **sql** - Sql command to execute.
* **connection** - JDBC Connection object.
* **properties** - Optional List[String] of bind variables.

## Close JDBC Connection
This step can be used to close the Connection object returned by *Get JDBC Connection*.

* **connection** - An open connection object.
