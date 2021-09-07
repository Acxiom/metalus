[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# AWSSecretsManagerCredentialProvider
This [CredentialProvider](../../docs/credentialprovider.md) implementation extends the [DefaultCredentialProvider](../../docs/credentialprovider.md#DefaultCredentialProvider)
by searching the AWS Secrets Manager for the named secret. An [AWSCredential](../../docs/credentialprovider.md#metalus-aws) will be returned
containing the string value. A region is required to instantiate.

## Secrets Manager Formats
When creating the secret that will be used, there are several properties that will be considered. The default method
for storing credentials is to place the key in the name field and the secret in the value field. The recommended approach
is to use key names so that they can specifically be targeted. Below is a list of keys that can be used:

### API Key and Secret
* accessKeyId - This is the key
* secretAccessKey - This is the secret
### Assume Role
* role - This is the name of the role
* accountId - The AWS account id
* session - A name to use for this assume role session
* partition - Used in the creation of the ARN. Defaults to 'aws'
* externalId - An optional unique id provided when using another account

### GCP
GCP keys may be stored using the api key / secret option. Each field of the JSON service account key needs to be
stored using the field name and value. The _com.acxiom.gcp.pipeline.GCPCredentialParser_ will need to be included
in order for parsing to happen appropriately.
