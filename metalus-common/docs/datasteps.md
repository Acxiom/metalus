[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# DataSteps
DataSteps provides the user with steps that can help process data. This includes grooupBy, join, union and adding columns.

##join()
This step will join two data frames together.
An optional string expression can be provided.
Consult the spark documents for a complete list of supported join types.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|left|Dataset[_]|left side of the join| n/a |
|right|Dataset[_]|right side of the join| n/a |
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
|dataFrame|Dataset[_]|data frame to group| n/a |
|groupings|List[String]|list of string expressions to group on|n/a|
|aggregations|List[String]|list of string aggregate expressions|n/a|

##union()
This step will perform a union operation.
The underlying implementation calls spark's union by name function, so column order is irrelevant.
UNION ALL and UNION DISTINCT behavior can be toggled, and will perform a DISTINCT by default.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|Dataset[_]|initial data frame| n/a |
|append|Dataset[_]|data frame to append| n/a |
|distinct|Boolean|flag to indicate whether a distinct should be performed|true|

## applyFilter()
This step will apply a filter to an existing data frame returning only rows that pass the expression criteria.  The expression
is passed as a String and acts much like a 'where' clause in a sql statement.  Any columns on the input dataframe can be used
in the expression.

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|Dataset[_]|a data frame containing data to be filtered| n/a |
|expression|String|the expression containing the filter criteria|n/a|

## addUniqueIdToDataFrame()
This step will add a unique Id to an existing dataframe (using the 'monatonically_increase_id' spark udf)

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|idColumnName|String|the name to use for the newly created attribute| n/a |
|dataFrame|Dataset[_]|the data frame to modify| n/a |

## addStaticColumnToDataFrame()
This step will add a static value to every row of an existing DataFrame

### Input Parameters
| Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|dataFrame|Dataset[_]|the data frame to modify| n/a |
|columnName|String|the name for the new attribute| n/a |
|columnValue|Any|the string value to set for the new attribute on each row| n/a |
|standardizeColumnName|Boolean|flag to control whether the column name should be standardized|true|
