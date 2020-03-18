[Documentation Home](readme.md)

# Http Rest Client
This object provides a simple client for communicating with restful services. The client requires a url and optional 
_Authorization_ class.

## Authorization
The _Authorization_ trait provides a single function named _authorize_ that is provided a URL Connection.

Any parameters required to instantiate the _Authorization_ instance need to be prefixed with _authorization._ so
they are identified and passed in. To use basic authorization, consider this class signature: 

```scala
class BasicAuthorization(username: String, password: String) extends Authorization
```
would need these command line parameters:
```
--authorization.class com.acxiom.pipeline.api.BasicAuthorization --authorization.username myuser --authorization.password mypasswd
```

## Functions

### Exists
This function will attempt to open a connection to the provided path to ensure it exists.

### Get Input Stream
This function will open an input stream to the provided path. Calling functions are required to close the stream which
will close the connection.

### Get Output Stream
This function will open an output stream to the provided path. Calling functions are required to close the stream which
will close the connection.

### Get String Content
This function will perform a GET against the provided path and return the body as a string.

### Post JSON Content
This function will take a given string and _POST_ it to a path as JSON.

### Put JSON Content
This function will take a given string and _PUT_ it to a path as JSON.

### Post String Content
This function will take a given string and _POST_ it to a path as the provided content type. The default is JSON.

### Put String Content
This function will take a given string and _PUT_ it to a path as the provided content type. The default is JSON.

### Delete
This function will attempt to run a DELETE operation against the provided path.

### Get Content Length
This function will open a connection to the provided path and return the content length.
