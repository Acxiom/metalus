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
