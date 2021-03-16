[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# AWSSecretsManagerCredentialProvider
This [CredentialProvider](../../docs/credentialprovider.md) implementation extends the [DefaultCredentialProvider](../../docs/credentialprovider.md#DefaultCredentialProvider)
by searching the AWS Secrets Manager for the named secret. A [BasicCredential](../../docs/credentialprovider.md#BasicCredential) will be returned
containing the string value. A region is required to instantiate.
