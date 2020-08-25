[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# JSONSteps
This step object provides generic steps for working with JSON. 
The following functions provided:

## Convert JSON String to Map
Given a JSON string, this step will convert it to a Map that can be passed to other steps. Using the built-in 
_dot notation_ supported by the step mapper, it should be easy to access any of that data elements.
Full parameter descriptions listed below:

* **jsonString** - The string containing the JSON.
* **formats** - Optional Json4s Formats object that will override the pipeline context formats.

## Convert JSON Map to JSON String
Given a map, this step will convert it to a JSON string that can be passed to other steps. This is useful when working
with [ApiSteps](apisteps.md) that require a JSON string for the content.
Full parameter descriptions listed below:

* **jsonMap** - The map to convert to a JSON string.
* **formats** - Optional Json4s Formats object that will override the pipeline context formats.

## Convert object to JSON String
Given an object, this step will convert it to a JSON string that can be passed to other steps. This is useful when working
with [ApiSteps](apisteps.md) that require a JSON string for the content.
Full parameter descriptions listed below:

* **obj** - The object to convert to a JSON string.
* **formats** - Optional Json4s Formats object that will override the pipeline context formats.

## Convert JSON String to object
Given a json string, this step will deserialize it to an object.

* **jsonString** - The JSON string to convert to an object.
* **objectName** - The fully qualified class name of the object.
* **formats** - Optional Json4s Formats object that will override the pipeline context formats.

## Convert JSON String to Schema
Given a JSON string, this step will convert it to a Schema that can be passed to other steps. This is useful when working
with [TransformationSteps](transformationsteps.md) that require a Schema.
Full parameter descriptions listed below:

* **schema** - The JSON string to convert to a Schema.
* **formats** - Optional Json4s Formats object that will override the pipeline context formats.

## Convert JSON String to DataFrame
Given a JSON string, this step will convert it to a DataFrame that can be passed to other steps. This step supports both
single line JSON (one JSON on each line as separate documents) as well as multi-line JSON 
(json documents wrapped in an array). This step is useful for small JSON strings.
Full parameter descriptions listed below:

* **jsonString** - The JSON string to convert to a DataFrame.

## Build JSON4S Formats
Provide lists of custom serializer classes and enumeration classes to build a new Formats object that can be used to
 override the Formats on the pipelineContext.
 
* **customSerializers** - List of custom serializer classes.
* **enumIdSerializers** - List of Enumeration classes to serialize by id.
* **enumNameSerializers** - List of Enumeration classes to serialize by name.
