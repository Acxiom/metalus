[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Streaming Query Monitor
Streaming Query Monitors provide a method for interacting with a running Spark StreamingQuery object. Implementations
can be created to perform different types of monitoring, control whether the query is stopped, whether to continue and
provide a map of variables that will be placed on the globals for the next step.
## BaseStreamingQueryMonitor
This implementation doesn't do anything but allow continuous streaming. The query doesn't actually get monitored or stopped.
## BatchWriteStreamingQueryMonitor
This abstract class provides a base implementation for streaming queries that need to write to disk. Basic status 
checking is provided and abstract functions that can be overridden to control behavior. This class uses the following
globals to determine behavior:
* **STREAMING_BATCH_MONITOR_TYPE** - Either _duration_ or _count_.
* **STREAMING_BATCH_MONITOR_DURATION** - A number representing the time amount
* **STREAMING_BATCH_MONITOR_DURATION_TYPE** - Either _milliseconds_, _seconds_ or _minutes_.
* **STREAMING_BATCH_MONITOR_COUNT** - A number representing the approximate number of records to process before stopping the query.
## BatchPartitionedStreamingQueryMonitor _(com.acxiom.pipeline.streaming.BatchPartitionedStreamingQueryMonitor)_
This implementation will process until the limit is reached and then stop the query. The query should continue and a new 
partition value will be provided on the globals. The partition value will be pushed to the global specified using the
_STREAMING_BATCH_PARTITION_GLOBAL_ global. **Note:** When using this monitor, the _checkpointLocation_ must be specified
so that a single location is used throughout.

* **STREAMING_BATCH_PARTITION_COUNTER**   - Track the number of times this has been invoked.
* **STREAMING_BATCH_PARTITION_GLOBAL**    - The name of the global key that contains the name of the partition value.
* **STREAMING_BATCH_PARTITION_TEMPLATE**  - Indicates whether to use a _counter_ or _date_.

The date sting will be formatted using this template: _yyyy-dd-MM HH:mm:ssZ_
## (Experimental) BatchFileStreamingQueryMonitor _(com.acxiom.pipeline.streaming.BatchFileStreamingQueryMonitor)_
This implementation will process until the limit is reached and then stop the query. The query should continue and a new
destination value will be provided on the globals. The destination will be modified by appending an underscore and the 
template value (counter or date). The destination global will then be updated with the new value.

The following parameters are required to use this monitor:
* **STREAMING_BATCH_OUTPUT_PATH_KEY** - Item in the path that needs to be tagged.
* **STREAMING_BATCH_OUTPUT_GLOBAL**   - Provides the global key to get the path being used for output.
* **STREAMING_BATCH_OUTPUT_TEMPLATE** - Indicates whether to use a _counter_ or _date_.
* **STREAMING_BATCH_OUTPUT_COUNTER**  - Track the number of times this has been invoked.

The date sting will be formatted using this template: _yyyy_dd_MM_HH_mm_ss_SSS_
