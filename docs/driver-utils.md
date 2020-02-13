[Documentation Home](readme.md)

# Driver Utilities
The driver utilities object provides helper functions that are useful when extending the base Metalus functionality.

## Create Spark Conf
This function is used to create a new _SparkConf_ and register classes that need to be serialized.

## Extract Parameters
This function takes the arguments array from the entry point of the driver class and creates a map of parameters. This 
function will look for parameters starting with two dashes (--) and grab the value. Additionally this function will 
validate required parameters.

## Get [Http Rest Client](httprestclient.md)
This function will take a URL and a map of parameters (provided by extract parameters) to initialize an HttpRestClient. 
This function handles parsing the authorization parameters as well.

## Validate Required Parameters
Given a parameter map and a list of required parameters, this function will validate that all required parameters exist 
in the map. A _RuntimeException_ will be thrown indicating all missing parameters.

## Parse Pipeline JSON
Given a JSON string, this function will convert it to a _Pipeline_ object.

## Parse JSON
This function will parse a JSON string into a valid object.

## Load JSON From File
This function will load a JSON from a file. It will attempt to determine the [FileManager](filemanager.md) based on the 
_fileLoaderClassName_ parameter.

## Add Initial DataFrame to Execution Plan
This function is used by streaming drivers to inject the DataFrame created from the stream into the execution plan.
