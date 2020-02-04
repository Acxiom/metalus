[Documentation Home](../../docs/readme.md) | [Mongo Home](../../readme.md)

# Mongo Steps
The MongoSteps object provides step functions for working with a Mongo database. There are several step functions 
provided:

## Read DataFrame from Mongo
This function will read a collection from Mongo into a DataFrame. Full parameter descriptions are listed below:

* **uri** - The URI of the database server
* **collectionName** - The name of the collection where to read the data

## Write DataFrame to Mongo
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to Mongo.
* **uri** - The URI of the database server
* **collectionName** - The name of the collection where to write the data
