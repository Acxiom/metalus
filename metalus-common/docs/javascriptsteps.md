# JavascriptSteps
This step object provides a way for the application developer to define steps at runtime using the Javascript language
without the need for writing and compiling Scala steps. This should only be used for simple step constructs such as 
branching steps or basic processing. Writing Javascript that interacts with Scala can be cumbersome, so two system 
objects are provided:

* **pipelineContext** - The current *PipelineContext* is provided to allow access to current state. This object is read only.
* **ReflectionUtils** - This utility allows the script to extract values from Scala objects including *Options*.
* **userValue** - Optional user provided value.

There are two step functions provided:

## Execute script 
This step function will simply execute the script and return a *PipelineStepResponse*.

## Execute script with value 
This step function will execute the script making the value available io the script with the variable name **userValue**
and return a *PipelineStepResponse*.

## Example
This example demonstrates how to read a file into a *DataFrame* using nothing but Javascript. This script assumes the 
step function that takes a provided value is being used.

Before the script can do anything it needs to have some of the Spark static objects imported:

```javascript
var MetaData = Java.type('org.apache.spark.sql.types.Metadata');
var StructType = Java.type('org.apache.spark.sql.types.StructType');
var StructField = Java.type('org.apache.spark.sql.types.StructField');
var DataTypes = Java.type('org.apache.spark.sql.types.DataTypes');
```

Now the script can create the schema to be used for the DataFrame using the structures imported above:

```javascript
var schema = new StructType(new Array(
	new StructField('id', DataTypes.LongType, true, MetaData.empty()),
	new StructField('first_name', DataTypes.StringType, true, MetaData.empty()),
	new StructField('last_name', DataTypes.StringType, true, MetaData.empty()),
	new StructField('email', DataTypes.StringType, true, MetaData.empty()),
	new StructField('gender', DataTypes.StringType, true, MetaData.empty()),
	new StructField('ein', DataTypes.StringType, true, MetaData.empty()),
	new StructField('postal_code', DataTypes.StringType, true, MetaData.empty())
));
```

Next the script needs to get a handle to the *SparkSession* and create a *DataFrameReader*:

```javascript
var sparkSession = pipelineContext.sparkSession().get();
var dfReader = sparkSession.read();
```

Tthe script can set the newly created schema on the reader, the separator character used by the file and disable 
inferring the schema:

```javascript
dfReader = dfReader.schema(schema).option('sep', ',').option("inferSchema", false)
```

Finally the script informs the reader that the file has a header, sets the format to 'csv' and calls the load function
to create the *DataFrame*:

```javascript
dfReader.option("header", true).format('csv').load(userValue);
```

Note that the *return* keyword is not used. The final statement output is used as the return automatically.

Here is the full script:

```javascript
var MetaData = Java.type('org.apache.spark.sql.types.Metadata');
var StructType = Java.type('org.apache.spark.sql.types.StructType');
var StructField = Java.type('org.apache.spark.sql.types.StructField');
var DataTypes = Java.type('org.apache.spark.sql.types.DataTypes');
var schema = new StructType(new Array(
	new StructField('id', DataTypes.LongType, true, MetaData.empty()),
	new StructField('first_name', DataTypes.StringType, true, MetaData.empty()),
	new StructField('last_name', DataTypes.StringType, true, MetaData.empty()),
	new StructField('email', DataTypes.StringType, true, MetaData.empty()),
	new StructField('gender', DataTypes.StringType, true, MetaData.empty()),
	new StructField('ein', DataTypes.StringType, true, MetaData.empty()),
	new StructField('postal_code', DataTypes.StringType, true, MetaData.empty())
));
var sparkSession = pipelineContext.sparkSession().get();
var dfReader = sparkSession.read();
dfReader = dfReader.schema(schema).option('sep', ',').option("inferSchema", false)
dfReader.option("header", true).format('csv').load(userValue);
```
