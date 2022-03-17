[Documentation Home](readme.md)

# Serialization
Metalus uses json4s to serialize json maps provided in the globals section of the application into objects.
By default, case classes with "native" types (String, Int, Long, etc.) are supported out of the box.
For more complicated objects containing traits or enumeration fields, custom serializers can be provided to the
application.

## json4sSerializers
This objects contains three lists of ClassInfo objects that are used to build out a Formats object for each execution.
These lists are:

* **customSerializers** - ClassInfo objects containing the class name of json4s custom serializers.
* **enumIdSerializers** - ClassInfo objects containing the fully qualified names of Enumeration objects. These will be
  used to instantiate EnumSerializers which will match integers to enum values.
* **enumNameSerializers** - ClassInfo objects containing the fully qualified names of Enumeration objects. These will be
  used to instantiate EnumNameSerializers which will match strings to enum values.
* **hintSerializers** - ClassInfo objects containing classnames tha should be used to deserialize objects containing the
  _jsonClass_ attribute.
  
Once built, the Formats are set on the pipelineContext as _json4sFormats_ and will be used by metalus for all further
serialization. For convenience, the Formats on the pipeline context can be accessed using the _getJson4sFormats_ method.
