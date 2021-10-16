[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# GCPSecretsManagerCredentialProvider
This [CredentialProvider](../../docs/credentialprovider.md) implementation extends the [DefaultCredentialProvider](../../docs/credentialprovider.md#DefaultCredentialProvider)
by searching the GCP Secrets Manager for the named secret. A [BasicCredential](../../docs/credentialprovider.md#BasicCredential) will be returned
containing the string value. A _projectId_ is required to instantiate.

## Secrets Manager Formats
When creating the secret that will be used, there are several properties that will be considered. Since GCP Secrets Manager
takes a single value, when storing the JSON service account key, it should be stored as JSON. The same is applicable when
storing [AWS keys](../../metalus-aws/docs/awssecretsmanager-credentialprovider.md#secrets-manager-formats). The data should
not be encoded.
