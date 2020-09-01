[Documentation Home](../../docs/readme.md) | [Kafka Home](../readme.md)

# KafkaSteps
KinesisSteps provides steps that allow writing a DataFrame to a Kinesis Stream.

## Write to Path By Key Field
This function will write a given DataFrame to the provided Kafka Topic. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **topic** - A valid Kafka topic
* **kafkaNodes** - A comma separated list Kafka brokers
* **keyField** - The name of the column to use for the key value for each row

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **clientId** - The Kafka Client Id

## Write to Path By Key
This function will write a given DataFrame to the provided Kafka Topic. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **topic** - A valid Kafka topic
* **kafkaNodes** - A comma separated list Kafka brokers
* **key** - The static key to use for each row

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **clientId** - The Kafka Client Id

## Post Message to Topic
This function will write a given message to the provided Kafka Topic. Full parameter descriptions are listed below:

* **message** - The message to post.
* **topic** - A valid Kafka topic
* **kafkaNodes** - A comma separated list Kafka brokers
* **key** - The static key to use for each row

*Optional Parameters:*
* **clientId** - The Kafka Client Id
