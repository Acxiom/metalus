# Application Example
This example will demonstrate how to use pipelines and step-groups stored in the metadata of the jar file using
application.json during submit.

One pipeline will be executed which calls a step-group to download a file to hdfs.  It then adds basic transformations
to the file adding a unique file id and a unique record id before writing to a "bronze" zone parquet file.  The pipeline
is access from the metadata stored in the metadata of metalus-common and all of the steps called by these pipelines can
be found in the steps library of metalus-common.

## Bronze Zone Traits
The "bronze" zone is intended to house data as close to the original file as possible.  To avoid data loss and complications
when introducing future intent that may change how the data is interpreted downstream, the "bronze" zone is setup as follows:

* stored with the names provided in the header cleaned up to make them query friendly (remove special characters)
* all data types should be STRING to avoid data loss during type conversion
* stored in it's own Parquet data to optimize downstream processing but does not have to conform to existing data
* stored by "fileId" which is provided as a global parameter and should be unique per file
* saveMode is "overwrite" to allow for clean restarts (avoids accidental duplication of data)

# Setup
In order to run this pipeline, an input file will need to be stored on an SFTP site that is reachable from the and all
the connection information will need to be made available through global parameters.

## Global Parameters
In order to run the pipelines, the following global parameters will need to be provided either in the application.json
file or as command line parameters during the spark-submit:

| Global Name | Type | Description | Default |
| --- |:---|:--- |:---:|
|fileId|String|the unique file id to use when storing this file| n/a |
|sftpHost|String|hostname for the sftp site where the incoming file is stored| n/a |
|sftpPort|Integer|port for the sftp site where the incoming file is stored| 22 |
|sftpUsername|String|username to use when connecting to the sftp site| n/a |
|sftpPassword|String|password to use when connecting to the sftp site| n/a |
|sftpInputPath|String|path on sftp site where the incoming file can be found (includes file name)| n/a |
|inputBufferSize|Integer|input buffer size setting for sftp transfer| 65536 |
|outputBufferSize|Integer|output buffer size setting for sftp transfer| 65536 |
|readBufferSize|Integer|read buffer size setting for sftp transfer| 32768 |
|landingPath|String|path in hdfs where the raw landed file will be stored (by fileId)| n/a |
|inputReaderOptions|DataFrameReaderOptions|dataframe reader options to use for reading the landed file| n/a |
|bronzeZonePath|String|path in hdfs where the parquet version of the file should be stored (by fileId)| n/a |

### Example application.json with All Required Globals
Below is an example of a completed application.json configuration file that can be used to run the
[pipeline](../../metalus-common/src/main/resources/metadata/pipelines/f4835500-4c4a-11ea-9c79-f31d60741e3b.json)
using the metadata found in common-steps
```json
{
    "sparkConf": {
        "kryoClasses": [
            "org.apache.hadoop.io.LongWritable",
            "org.apache.http.client.entity.UrlEncodedFormEntity"
        ],
        "setOptions": [
            {
                "name": "spark.hadoop.io.compression.codecs",
                "value": "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec"
            }
        ]
    },
    "stepPackages": [
        "com.acxiom.pipeline.steps",
        "com.acxiom.pipeline"
    ],
    "globals": {
        "sftpHost": "myhost",
        "sftpUsername": "myUser",
        "sftpPassword": "myPa$$",
        "sftpInputPath": "myFiles/demo_file.csv",
        "landingPath": "/Users/myUser/landingZone/",
        "fileId": "file-id-001",
        "inputReaderOptions": {
            "className": "com.acxiom.pipeline.steps.DataFrameReaderOptions",
            "object": {
                "format": "csv",
                "options": {
                    "header": "true",
                    "delimiter": ","
                }
            }
        },
        "bronzeZonePath": "/Users/myUser/bronzeZone/"
    },
    "executions": [
        {
            "id": "DownloadToBronzeHdfs",
            "pipelineIds": [
                "f4835500-4c4a-11ea-9c79-f31d60741e3b"
            ]
        }
    ]
}
```

### Example Spark Submit
Below is an example of a spark-submit call using an application.json file similar to above:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master local \
--deploy-mode client \
--jars <jar_path>/metalus-common_2.11-spark_2.3-<VERSION>.jar \
<jar_path>/metalus-application_2.11-spark_2.3-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <path_to_application.json> \
--logLevel INFO
```

_**Note**: it may make sense to send some of the global parameters as command line parameters (eg. --fileId file-id-001)_ 


## Expected Results
Upon successful completion of a the pipeline, two new data sets should be available:

* In the _$landingPath_ location, a new file named _$fileId_ should exist
    * should be a copy of what was on the sftp site
* In the _$bronzeZonePath_ location, a new folder named _$fileId_ should exist containing one or more parquet files
    * there should be 2 new attributes on the dataframe (METALUS_RECORD_ID, METALUS_FILE_ID)
    * all data from the original file should exist in this dataframe with names that are converted to "safe" column names
