[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# SparkConfigurationSteps
This object exposes some basic functions to set configurations on the spark context at run time.

##Set Spark Local Property
Set a local property on the current thread.

* **key** - The key of the property to set.
* **value** - The value to set. Use None to remove the property.

##Set Spark Local Properties
Set a local property on the current thread for each entry in the properties map.

* **properties** - A Map where each entry will be set as a key/value pair.
* **keySeparator** - Replaces all occurrences of this string with periods in the keys. Default is __.

##Set Hadoop Configuration Property
Set a property on the hadoop configuration.

* **key** - The key of the property to set.
* **value** - The value to set. Use None to remove the property.

##Set Hadoop Configuration Properties
Set a property on the hadoop configuration for each entry in the properties map.

* **properties** - A Map where each entry will be set as a key/value pair.
* **keySeparator** - Replaces all occurrences of this string with periods in the keys. Default is __.

##Set Job Group
Set a job group id and description to group all upcoming jobs on the current thread.

* **groupId** - The name of the group.
* **description** - Description of the group.
* **interruptOnCancel** - When true, then job cancellation will result in Thread.interrupt()
 getting called on the job's executor threads
 
##Clear Job Group
Clears the current job group.
