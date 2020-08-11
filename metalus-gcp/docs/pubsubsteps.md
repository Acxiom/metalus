[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# PubSubSteps
PubSubSteps provides steps that allow writing a DataFrame to a Pub/Sub Topic.

## Write DataFrame to Pub/Sub with Credentials
This function will write a given DataFrame to the provided topic. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **topicName** - The Pub/Sub topic where data will be written

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **credentials** - The GCP credentials to use when connecting

## Write DataFrame to Pub/Sub using Global Credentials
This function will write a given DataFrame to the provided topic. This version will attempt to pull credentials from the
CredentialProvider in the PipelineContext. Full parameter descriptions listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **topicName** - The Pub/Sub topic where data will be written

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message

## Write message  to Pub/Sub
This function will write a message to the provided topic. Full parameter descriptions are listed below:

* **message** - The message to post
* **topicName** - The Pub/Sub topic where data will be written

*Optional Parameters:*
* **credentials** - The GCP credentials to use when connecting

## Write to Pub/Sub using Global Credentials
This function will write a message to the provided topic. This version will attempt to pull credentials from the
CredentialProvider in the PipelineContext. Full parameter descriptions listed below:

* **message** - The message to post
* **topicName** - The Pub/Sub topic where data will be written
