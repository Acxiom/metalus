[Documentation Home](../docs/readme.md)

# Metalus GCP Step Library
The Metalus GCP is a step library with specific steps and utilities for working with GCP technologies.

## Authorization
Support for GCS has been included in this library. By default, GCS and BigQuery will be authorized if credential
information is provided. Additionally, for GCS the file system will be set unless the _skipGCSFS_ parameter is set to true
on the command line or as a global.

## Step Classes
* [GCSSteps](docs/gcssteps.md)
* [PubSubSteps](docs/pubsubsteps.md)
* [BigQuerySteps](docs/bigquerysteps.md) **_Experimental_**

## Extensions
* [GCSFileManager](docs/gcsfilemanager.md)
* [PubSub Pipeline Driver](docs/pubsubpipelinedriver.md)
* [GCP Secrets Manager Credential Provider](docs/gcpsecretsmanager-credentialprovider.md)
