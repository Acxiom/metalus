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


## Available Steps
There are multiple steps that are exposed in the QuerySteps library:  

##### dataFrameToTempView()
Stores an existing *dataframe* as a Spark temporary view that can be accessed in future steps in the same session using
Spark SQL queries.  If a *viewName* is provided, it will be used to name the view, otherwise a system generated unique name
will be used.  In either case, the viewName used to store the dataframe will be returned from this step.

##### queryToTempView()
Runs the provided *query* against Spark temporary views that were made available in previous steps from this session.  A
*variableMap* parameter can be provided to allow for variable replacement in the query provided.  If a viewName is provided,
it will be used to name the view, otherwise a system generated unique name will be used.  In either case, the viewName 
used to store the dataframe will be returned from this step.

##### queryToDataFrame()
Runs the provided query against Spark temporary views that were made available in previous steps from this session.  A
*variableMap* parameter can be provided to allow for variable replacement in the query provided.  The resulting dataframe
will be returned from this step.

##### tempViewToDataFrame()
Pulls an existing Spark temporary view (created in the current session) with the *viewName* provided into a new dataframe.

##### dataFrameQueryToTempView()
Shortcut step that allows a dataframe to be loaded, queried, and stored as a Spark temporary view for future queries.  As with previous steps,
a *variableMap* can be provided for variable replacement inside the query.  The *inputViewName* must be provided and should
match the name used in the *query* to reference the dataframe.  If the *outputViewName* parameter is provided, it will be
used to name the final temporary view, otherwise the system will generate a unique name.  In either case, the final viewName
is returned from this step.

##### dataFrameQueryToDataFrame()
Shortcut step tha allows a dataframe to be loaded, queried, and returned as a new dataframe.  As with previous steps,
a *variableMap* can be provided for variable replacement inside the query.  The *inputViewName* must be provided and should
match the name used in the *query* to reference the dataframe.  The dataframe created from the query is returned from this
step.

##### cacheTempView()
This step will cache the results of an existing temporary view named with the *viewName* provided.

  