[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# ApiSteps
This step object provides generic steps for working with REST APIS. 
There are two step functions provided:

## Create HttpRestClient
Given a host url and optional *Authorization*, this step will create a HttpRestClient.
Full parameter descriptions are listed below:

* **hostUrl** - The fully qualified host URL.
* **authorization** - Optional *Authorization* instance that will provide authorization for REST calls.

## Create HttpRestClient
Given a protocol, host, port and optional *Authorization*, this step will create a HttpRestClient.
Full parameter descriptions are listed below:

* **protocol** - The protocol for the URL. Restricted to http and https.
* **host** - The host name for the URL.
* **port** - The port to use for the URL.
* **authorization** - Optional *Authorization* instance that will provide authorization for REST calls.

## Exists
This function will attempt to open a connection to the provided path to ensure it exists.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.

### Get Last Modified Date
This function will open a connection to the provided path and return the last modified date.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.

### Get Content Length
This function will open a connection to the provided path and return the content length.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.

### Get Headers
This function will open a connection to the provided path and return the headers.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.

## Get Input Stream
This function will open an input stream to the provided path. Calling functions are required to close the stream.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.
* **bufferSize** - Optional buffer size.

## Get Output Stream
This function will open an output stream to the provided path. Calling functions are required to close the stream.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.
* **bufferSize** - Optional buffer size.

## Get String Content
This function will perform a GET against the provided path and return the body as a string.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.

## Post String Content
This function will take a given string and _POST_ it to a path as the provided content type. The default is JSON.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.
* **contentType** - Optional content type. Default is "application/json".

### Put String Content
This function will take a given string and _PUT_ it to a path as the provided content type. The default is JSON.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.
* **contentType** - Optional content type. Default is "application/json".

### Delete
This function will attempt to run a DELETE operation against the provided path.

Full parameter descriptions are listed below:
* **httpRestClient** - The HttpRestClient to use when making calls.
* **path** - The path to operate against.
