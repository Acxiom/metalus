[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# GCPSecretsManagerCredentialProvider
This [CredentialProvider](../../docs/credentialprovider.md) implementation extends the [DefaultCredentialProvider](../../docs/credentialprovider.md#DefaultCredentialProvider)
by searching the GCP Secrets Manager for the named secret. A [BasicCredential](../../docs/credentialprovider.md#BasicCredential) will be returned
containing the string value. A _projectId_ is required to instantiate.
