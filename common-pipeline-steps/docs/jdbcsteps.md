# JDBCSteps
This step object provides a way to read and write via JDBC. A case class named JDBCStepOptions is provided 
that accepts a url, a table name, and a Map[String, String] of properties.
There are six step functions provided:

##Read With JDBCOptions
Given a JDBCOptions object, this step will read a table into a DataFrame. Full parameter descriptions are listed below:

* **jDBCOptions** - A JDBCOptions object used to connect.

## Read With StepOptions
Given a JDBCStepOptions object, this step will read a table into a DataFrame. Full parameter descriptions are listed below:

* **jDBCStepOptions** - A JDBCStepOptions object used to connect.

## Read With Properties
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
Given a JDBCStepOptions object and a DataFrame, this step will write to a table via JDBC. 
Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written via JDBC.
* **jDBCStepOptions** - A JDBCStepOptions object used to connect.
* **saveMode** - The Writing behavior. Valid values: (Append, Overwrite, ErrorIfExists, Ignore). 
The default value is "_Overwrite_".

## Write With StepOptions
Given a url and table name, this step will write to a table via JDBC. 
Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written via JDBC.
* **url** - A valid jdbc url.
* **table** - A table name or sub query
* **connectionProperties** - Optional Map[String,String] of properties for the given format.
* **saveMode** - The Writing behavior. Valid values: (Append, Overwrite, ErrorIfExists, Ignore). 
The default value is "_Overwrite_".