[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# CredentialSteps
This step object provides access to [Credential](../../docs/credentialprovider.md) objects within a pipeline. This is 
useful when the name of the credential isn't known until runtime or the pipeline has been added to an application.

## Get Credential
Given the credential name, this step will return a credential or None.

Full parameter descriptions listed below:

* **credentialName** - The name of the credential to retrieve.
