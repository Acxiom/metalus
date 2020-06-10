[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# ScalaSteps
This step object provides a way for the application developer to define steps at runtime using the Scala language
without the need for writing and compiling steps. This should only be used for simple step constructs such as 
branching steps or basic processing. A PipelineContext object is provided for use in the script, bound as "pipelineContext".
A loggger can also be referenced, bound as "logger"
There are three step functions provided:

## Execute script 
This step function will simply execute the script and return a *PipelineStepResponse*.
Full parameter descriptions are listed below:
* **script** - The script to execute.

## Execute script with value 
This step function will execute the script making the value available in the script with the variable name **userValue**
and return a *PipelineStepResponse*.
Full parameter descriptions are listed below: 
* **script** - The script to execute.
* **value** - The value that will be bound in the script as "userValue".
* **type** - Optional type name can be used to explicitly specify the type of "userValue".

## Execute script with values
This step function will execute a script and bind each key/value pair in the provided values map to be usable in the script.
Returns a *PipelineStepResponse*.
Full parameter descriptions are listed below:
* **script** - The script to execute.
* **values** - Map of values to bind. Each key value pair will be bound to the script using the key.
* **types** - Optional map of type name overrides, used to explicitly specify the type of the values in the values map.

## Example
This example demonstrates how to read a file into a *DataFrame* using nothing but Scala. This script assumes the 
step function that takes a provided value is being used.

Before the script can do anything it needs to have some of the Spark static objects imported:

```scala
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql._
```

Now the script can create the schema to be used for the DataFrame using the structures imported above:

```scala
val schema = StructType(List[StructField](
  StructField("id", DataTypes.LongType, true, Metadata.empty),
  StructField("first_name", DataTypes.StringType, true, Metadata.empty),
  StructField("last_name", DataTypes.StringType, true, Metadata.empty),
  StructField("email", DataTypes.StringType, true, Metadata.empty),
  StructField("gender", DataTypes.StringType, true, Metadata.empty),
  StructField("ein", DataTypes.StringType, true, Metadata.empty),
  StructField("postal_code", DataTypes.StringType, true, Metadata.empty)
))
```

Next the script needs to get a handle to the *SparkSession* and create a *DataFrameReader*:

```scala
val sparkSession = pipelineContext.sparkSession.get
var dfReader = sparkSession.read
```

Tthe script can set the newly created schema on the reader, the separator character used by the file and disable 
inferring the schema:

```scala
dfReader = dfReader.schema(schema).option("sep", ",").option("inferSchema", false)
```

Finally the script informs the reader that the file has a header, sets the format to 'csv' and calls the load function
to create the *DataFrame*:

```scala
dfReader.option("header", true).format('csv').load(userValue);
```

Note that the *return* keyword is not used. The final statement output is used as the return automatically.

Here is the full script:

```scala
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql._
val schema = StructType(List[StructField](
  StructField("id", DataTypes.LongType, true, Metadata.empty),
  StructField("first_name", DataTypes.StringType, true, Metadata.empty),
  StructField("last_name", DataTypes.StringType, true, Metadata.empty),
  StructField("email", DataTypes.StringType, true, Metadata.empty),
  StructField("gender", DataTypes.StringType, true, Metadata.empty),
  StructField("ein", DataTypes.StringType, true, Metadata.empty),
  StructField("postal_code", DataTypes.StringType, true, Metadata.empty)
))
val sparkSession = pipelineContext.sparkSession.get
var dfReader = sparkSession.read
dfReader = dfReader.schema(schema).option("sep", ",").option("inferSchema", false)
dfReader.option("header", true).format("csv").load(userValue)
```
