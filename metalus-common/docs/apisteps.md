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
