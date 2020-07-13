[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# PubSubSteps
PubSubSteps provides steps that allow writing a DataFrame to a Pub/Sub Topic.

## Write to Path
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **topicName** - The Pub/Sub topic where data will be written

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **credentials** - The GCP credentials to use when connecting
