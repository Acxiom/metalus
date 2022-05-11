[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Streaming Monitor
This pipeline is designed to be chained with other pipelines that may produce a streaming DataFrame. The _STREAMING_DATAFRAME_
global is used to retrieve the DataFrame that will be monitored. This pipeline is useful as-is when the _streaming-job_
command line parameter is set to true.

## General Information
**Id**: _streaming-monitor_

**Name**: _Streaming Monitor_

## Required Parameters (all parameters should be part of the globals)
* **STREAMING_DATAFRAME** - The DataFrame to be monitored.
* **streamingMonitorClassName** - The **optional** fully qualified class name of the [streaming monitor class](./streamingquerymonitor.md) to use for the query.
