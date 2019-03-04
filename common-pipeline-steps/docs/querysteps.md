# QuerySteps
QuerySteps provides the user with a way to run queries in spark using Spark TempViews to store logic in the current
session.  This library provides steps to load dataframes into a Spark TempView and run queries against those
TempViews to create new TempViews or DataFrames.  This makes it possible for users to chain multiple queries
together to apply complicated logic using SQL expressions.

It is important to note that tables referenced in the provided queries must reference TempViews created in the current
session (i.e. DAG).  The user must keep track of these names in their chained queries.  

## TempView Names
When creating a TempView in any provided QueryStep, the user can provide the name to use when storing the TempView or
they can leave it up to the code to create a random unique name.  Whether the name is provided or not, the name used is
returned from these steps so they can be referenced in subsequent queries.

## Variable Replacement
In each QueryStep that accepts a query string, variable replacement can be performed by providing a key/value pair in the
'variableMap' parameter.  The steps will look for each key provided wrapped in "${}" and replace it with the value from
'variableMap'.

## Examples:
#### Basic Example:
* Query: "select * from ${tableName} where client_group = '${client_group}'"
* VariableMap = "tableName" -> "myTable", "client_group" -> "abcd"
* FinalQuery = "select * from myTable" where client_group = 'abcd'"

#### Using the Table Name for a Previous Step
* Query: "select * from ${tableName}"
* VariableMap: "tableName" -> "@step4"  (where step for is a queryToTempView() or dataFrameToTempView() step)

