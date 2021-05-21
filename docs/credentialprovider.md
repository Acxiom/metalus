[Documentation Home](readme.md)

# Credential Provider
The _CredentialProvider_ provides a single point of access to the Metalus application for obtaining credentials. The 
provider requires a map of parameters that may be used to initialize _Credentials_. A single function named _getNamedCredential_
is provided to perform look-ups.

## DefaultCredentialProvider
A default implementation is provided that will scan the parameters map for _credential-classes_. This is a comma 
separated list of fully qualified class names of _Credential_s that should be loaded. Each _Credential_ will be supplied
the same parameters Map used by the provider.

## Credential
A _Credential_ is used to provide custom credentials. Each implementation must have a name that can be used by the 
_CredentialProvider_ to return the custom _Credential_.

## DefaultCredential
This implementation will look in the provided parameters for _credentialName_ to set the name of the credential and
_credentialValue_ for the value.

# Metalus AWS
A trait named _AWSCredential_ is provided with the basic requirements for a credential: api key and secret. Implementations
should extend this trait and override the _awsAccessKey_ and _awsAccessSecret_ functions. 

Implementations provided:
* **com.acxiom.aws.utils.DefaultAWSCredential** - Reads the values for the name from _credentialName_ and splits the
_credentialValue_ on the _:_ character to populate the key and secret.
* **com.acxiom.aws.utils.AWSBasicCredential** - Reads the _accessKeyId_ and _secretAccessKey_ parameters to populate 
the functions. The name used to access this credential is _AWSCredential_.
* **com.acxiom.aws.utils.AWSCloudWatchCredential** - Reads the _cloudWatchAccessKeyId_ and _cloudWatchSecretAccessKey_ 
parameters to populate the functions. The name used to access this credential is _AWSCloudWatchCredential_.
* **com.acxiom.aws.utils.AWSDynamoDBCredential** - Reads the _dynamoDBAccessKeyId_ and _dynamoDBSecretAccessKey_ 
parameters to populate the functions. The name used to access this credential is _AWSDynamoDBCredential_.
* **[com.acxiom.aws.pipeline.AWSecretsManagerCredentialProvider](../metalus-aws/docs/awssecretsmanager-credentialprovider.md)** -An extension of
  DefaultCredentialProvider which will use the AWS secrets manager to attempt to find Credentials.
  
# Metalus GCP
A trait named _GCPCredential_ is provided with the basic requirements for a credential: A map of Strings. Implementations
should extend this trait and override the _authKey_ function. The _PubSubPipelineDriver_ will attempt to access this 
credential using the name _GCPCredential_.

Implementations provided:
* **com.acxiom.gcp.pipeline.DefaultGCPCredential** - GCPCredential implementation that looks for the gcpAuthKeyArray parameter to generate the authKey.
* **com.acxiom.gcp.pipeline.Base64GCPCredential** - Reads the credentials from a parameter named _gcpAuthKey_ and expects 
it to be Base64 encoded. It is only recommended for development testing and not production use.
* **[com.acxiom.gcp.pipeline.GCPSecretsManagerCredentialProvider](../metalus-gcp/docs/gcpsecretsmanager-credentialprovider.md)** -An extension of
DefaultCredentialProvider which will use the GCP secrets manager to attempt to find Credentials. 
