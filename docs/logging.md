[Documentation Home](readme.md)

# Logging
The [DriverSetup](pipeline-drivers.md#driversetup) class will attempt to set logging options based on three parameters:

* logLevel - Controls log level for all classes in the _com.acxiom_ package.
* rootLogLevel - Controls log level for all other classes.
* customLogLevels - Provides log levels for individual classes/packages. 

The values for _logLevel_ and _rootLogLevel_ will be valid Log4J log levels. Custom Log Levels is a comma separated
list of class/package and level pairs. Below is an example showing the proper format:

```shell
--customLogLevels com.acxiom.pipeline:WARN,org.apache.spark:ERROR,com.acxiom.pipeline.DriverSetup:TRACE
```

These options may be passed with the _spark-submit_ command's application parameters by prepending **--** before the name.
