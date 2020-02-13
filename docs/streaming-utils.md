[Documentation Home](readme.md)

# Streaming Utilities
This utility object provides serveral functions useful for working with Spark Streaming API.

## Create Streaming Context
Two functions are provided to build out a streaming context. Both require a SparkContext, but each differs in how duration
is configured. One function takes a _Duration_ object, while the other takes a string type and string duration.

## Get Duration
Given a string type and string duration, this function will return a _Duration_ object.

## Set Termination State
This function takes streaming context and a parameter map, this function will determine if the context should terminate
after a period of time or wait until the process is killed. If the parameters map has the _terminationPeriod_ key,
then the value will be converted to a long and treated as milliseconds. The context will end at once time has expired.
