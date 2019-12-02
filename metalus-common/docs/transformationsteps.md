### TransformationSteps
TransformationSteps provides the user with steps that can help transform data into a pre-defined schema or integrate into an existing
data frame.  This includes reordering columns, adding placeholders for missing columns, applying transformations from input to output,
standardizing column names, and converting column data types to match the destination.

Instructions for alternate column names on input or transforms are stored in a 'Mappings' object explained in more
detail below:

#### Transformation Object
A Transformations object consists of instructions for basic transformation tasks and can be included to provide additional flexibility in the
mapping process.  It includes the following root parameters:
* *__columnDetails__* - a list of column level transformation rules (see ColumnDetails section below for more information)
* *__filter__* - a string containing filter requirements in the form of a sql 'where' clause expression
* *__standardizeColumnNames__* - a boolean flag specifying whether column names should be standardized on output

A ColumnDetails record can be created for any column that may require manipulation to map to the desired value.  

*Note:* Only columns that require special handling need to be included in this list.  There is no need to create objects for
columns that don't need special handling.  Also, this list can be left out entirely with no repercussions. 

#### Parameters List for ColumnDetails
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

#### Transformations Example
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

#### Available Transformation Steps
There are multiple steps that are exposed in the TransformationSteps library:  

#### mapToDestinationSchema()
This step will map a data to an existing schema (StructType).  The output will be mapped to the destination schema honoring
data types, column order (including placeholders for missing columns), filters, and any input aliases and transformation that might be
provided in the Transformation object.

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|inputDataFrame|DataFrame|a data frame containing data to be mapped to destination| n/a |
|destinationSchema|Schema|the schema that the new data should conform to| n/a |
|transforms|Transformations|the object containing transforms and input aliases|Transformations(List())|
|addNewColumns|Boolean|a flag representing whether new columns on input (not on destination) should be added to output|true|    

#### mapToDestinationDataFrame()
This step will map a data to an existing data frame ensuring schema compatibility allowing new data to be saved safely with
existing data. The output will be mapped to the destination schema honoring data types, column order (including placeholders
for missing columns), filters, and any input aliases and transformation that might be provided in the Transformations object.

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|inputDataFrame|DataFrame|a data frame containing data to be mapped to destination| n/a |
|destinationDataFrame|DataFrame|the data frame that the new data should conform to| n/a |
|transforms|Transformations|the object containing transforms and input aliases|Transformations(List())|
|addNewColumns|Boolean|a flag representing whether new columns on input (not on destination) should be added to output|true|    

#### mergeDataFrames()
This step will map a data to an existing data frame and merge the new data frame safely with destination data frame. The output
will be a combination of the inputDataFrame (w/transforms, etc...) and the destinationDataFrame.

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|inputDataFrame|DataFrame|a data frame to be merged with destination| n/a |
|destinationDataFrame|DataFrame|the data frame that the new data should conform to| n/a |
|transforms|Transformations|the object containing transforms and input aliases|Transformations(List())|
|addNewColumns|Boolean|a flag representing whether new columns on input (not on destination) should be added to output|true|    

#### applyTransforms()
This step will apply provided transforms to a data frame honoring any input aliases prior to running expressions. The output
will include all fields from the original data frame with aliases and transform expressions applied.  It will also include new columns
if the Transformations object includes expressions for columns that do not exist, yet.

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame containing data to be transformed| n/a |
|transforms|Transformations|the object containing transforms and input aliases|n/a|

#### applyFilter()
This step will apply a filter to an existing data frame returning only rows that pass the expression criteria.  The expression
is passed as a String and acts much like a 'where' clause in a sql statement.  Any columns on the input dataframe can be used
in the expression.

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame containing data to be filtered| n/a |
|expression|String|the expression containing the filter criteria|n/a|

#### standardizeColumnNames()
This step will standardize the column names on the dataframe provided.  Standardization includes only replacing non-alphanumeric
and non-underscore characters (including whitespace) with an underscore (removing duplicate underscores from the final name)

##### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|DataFrame|a data frame containing columns to be standardized| n/a |