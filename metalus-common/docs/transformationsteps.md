[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# TransformationSteps
TransformationSteps provides the user with steps that can help transform data into a pre-defined schema or integrate into an existing
data frame.  This includes reordering columns, adding placeholders for missing columns, applying transformations from input to output,
standardizing column names, and converting column data types to match the destination.

Instructions for alternate column names on input or transforms are stored in a 'Mappings' object explained in more
detail below:

## Transformation Object
A Transformations object consists of instructions for basic transformation tasks and can be included to provide additional flexibility in the
mapping process.  It includes the following root parameters:
* *__columnDetails__* - a list of column level transformation rules (see ColumnDetails section below for more information)
* *__filter__* - a string containing filter requirements in the form of a sql 'where' clause expression
* *__standardizeColumnNames__* - a boolean flag specifying whether column names should be standardized on output

A ColumnDetails record can be created for any column that may require manipulation to map to the desired value.  

*Note:* Only columns that require special handling need to be included in this list.  There is no need to create objects for
columns that don't need special handling.  Also, this list can be left out entirely with no repercussions. 

## Parameters List for ColumnDetails
* *__outputField__* - stores the name of the field to be output 
    * must match destination name exactly if mapping to existing data frame or schema
    * new columns can be created using transforms with a new outputField name
* *__inputAliases__* - contains a list of alternate column names on the input that should be mapped to the outputField
    * must be unique across all column names and other aliases in a destination data frame or schema
* *__expression__* - stores spark sql expression to be applied prior to outputting a column
    * any column names referenced in the transform should exist on the input data frame to avoid spark errors
    * can include any valid spark sql functions
    * can include any fields on the data frame
    * no type validation is included in the current version

## Transformations Example
The following example shows how a Mappings object might be created to map and transform incoming data.

In this specific example, any input columns with named "first_name", "fname", or "firstname" will be mapped to column "first_name"
on the output and will have the "initcap" function applied to the value.  Also, a new field will be generated called "new_column" that
that will be created using the provided transform.  A basic filter to remove any empty customer_ids will be applied to the output
dataframe and column names will not be standardized.

```json
{
  "columnDetails": [
     {
        "outputField": "first_name",
        "inputAliases": ["fname", "firstname"],
        "transform": "initcap(first_name)"
     },
     {
        "outputField": "new_column",
        "inputAliases": [],
        "transform": "concat(initcap(first_name), ' ', initcap(last_name)"
     }
  ],
  "filter": "customer_id is not NULL",
  "standardizeColumnNames": false
}
```

## Available Transformation Steps
There are multiple steps that are exposed in the TransformationSteps library:  

## mapToDestinationSchema()
This step will map a data to an existing schema (StructType).  The output will be mapped to the destination schema honoring
data types, column order (including placeholders for missing columns), filters, and any input aliases and transformation that might be
provided in the Transformation object.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|inputDataFrame|DataFrame|a data frame containing data to be mapped to destination| n/a |
|destinationSchema|Schema|the schema that the new data should conform to| n/a |
|transforms|Transformations|the object containing transforms and input aliases|Transformations(List())|
|addNewColumns|Boolean|a flag representing whether new columns on input (not on destination) should be added to output|true|    

## mapToDestinationDataFrame()
This step will map a data to an existing data frame ensuring schema compatibility allowing new data to be saved safely with
existing data. The output will be mapped to the destination schema honoring data types, column order (including placeholders
for missing columns), filters, and any input aliases and transformation that might be provided in the Transformations object.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|inputDataFrame|DataFrame|a data frame containing data to be mapped to destination| n/a |
|destinationDataFrame|DataFrame|the data frame that the new data should conform to| n/a |
|transforms|Transformations|the object containing transforms and input aliases|Transformations(List())|
|addNewColumns|Boolean|a flag representing whether new columns on input (not on destination) should be added to output|true|    

## mergeDataFrames()
This step will map a data to an existing data frame and merge the new data frame safely with destination data frame. The output
will be a combination of the inputDataFrame (w/transforms, etc...) and the destinationDataFrame.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|inputDataFrame|DataFrame|a data frame to be merged with destination| n/a |
|destinationDataFrame|DataFrame|the data frame that the new data should conform to| n/a |
|transforms|Transformations|the object containing transforms and input aliases|Transformations(List())|
|addNewColumns|Boolean|a flag representing whether new columns on input (not on destination) should be added to output|true|    

## applyTransforms()
This step will apply provided transforms to a data frame honoring any input aliases prior to running expressions. The output
will include all fields from the original data frame with aliases and transform expressions applied.  It will also include new columns
if the Transformations object includes expressions for columns that do not exist, yet.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame containing data to be transformed| n/a |
|transforms|Transformations|the object containing transforms and input aliases|n/a|

## selectExpressions()
This step will select a list of expressions from an existing data frame.
 The expressions passed are strings and behave like a 'select' clause in a sql statement.
 Any columns on the input dataFrame can be used, and new columns can be added.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame to select from| n/a |
|expressions|List[String]|the list of expressions to select|n/a|

## addColumn()
This step will append a new column to the end of the provided dataFrame.
 The expression is passed as a String and can be any valid spark sql expression.
 
### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame to append the column to| n/a |
|columnName|String|name of the column to add|n/a|
|expression|String|spark sql expression for the column value|n/a|

## addColumns()
This step will append a new column to the end of the provided dataFrame.
 The map passed should be a key value pair of column names and expressions
 
### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame to append the columns to| n/a |
|columns|Map[String,String]|Map of column name/expressions|n/a|

##dropColumns()
This step will return a data frame minus the provided column names.
 Any column names not already on the dataFrame will be ignored.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame to drop columns from| n/a |
|columnNames|List[String]|List of columns to drop.|n/a|

##join()
This step will join two data frames together.
 An optional string expression can be provided.
 Consult the spark documents for a complete list of supported join types.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|left|DataFrame|left side of the join| n/a |
|right|DataFrame|right side of the join| n/a |
|expression|String|optional join expression|n/a|
|leftAlias|String|alias for the left side of the join|left|
|rightAlias|String|alias for the right side of the join|right|
|joinType|String|type of join to perform| inner|

##groupBy()
This step will perform a group by operation on the provided data frame.
The resulting DataFrame will have a combination of the grouping expressions and aggregations provided.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|data frame to group| n/a |
|groupings|List[String]|list of string expressions to group on|n/a|
|aggregations|List[String]|list of string aggregate expressions|n/a|

##union()
This step will perform a union operation. 
The underlying implementation calls spark's union by name function, so column order is irrelevant.
UNION ALL and UNION DISTINCT behavior can be toggled, and will perform a DISTINCT by default.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|initial data frame| n/a |
|append|DataFrame|data frame to append| n/a |
|distinct|Boolean|flag to indicate whether a distinct should be performed|true|

## applyFilter()
This step will apply a filter to an existing data frame returning only rows that pass the expression criteria.  The expression
is passed as a String and acts much like a 'where' clause in a sql statement.  Any columns on the input dataframe can be used
in the expression.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame containing data to be filtered| n/a |
|expression|String|the expression containing the filter criteria|n/a|

## standardizeColumnNames()
This step will standardize the column names on the dataframe provided.  Standardization includes only replacing non-alphanumeric
and non-underscore characters (including whitespace) with an underscore (removing duplicate underscores from the final name)

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame containing columns to be standardized| n/a |

#### addUniqueIdToDataFrame()
This step will add a unique Id to an existing dataframe (using the 'monatonically_increase_id' spark udf)

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|idColumnName|String|the name to use for the newly created attribute| n/a |
|dataFrame|DataFrame|the data frame to modify| n/a |

#### addStaticColumnToDataFrame()
This step will add a static value to every row of an existing dataframe)

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|the data frame to modify| n/a |
|columnName|String|the name for the new attribute| n/a |
|columnValue|String|the string value to set for the new attribute on each row| n/a |
