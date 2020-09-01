[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# S3FileManager
Provides a [FileManager](../../docs/filemanager.md) implementation that works with the GCS file system.  Full
parameter descriptions are listed below:

## Storage Configuration
* **bucket** - The GCS bucket being used.
* **storage** - The GCS Storage object to use.

## Parameterized Configuration
* **projectId** - The Project Id that owns the bucket.
* **bucket** - The GCS bucket being used.
* **jsonAuth** - The JSON service account credential file as a string.

## Credential Configuration
* **bucket** - The GCS bucket being used.
* **credentials** - The JSON service account credential file as a map.
