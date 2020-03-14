[Documentation Home](readme.md) | [Metalus Utils Home](../metalus-utils/readme.md)

# Dependency Manager
The dependency manager is a tool used to resolve dependent jars for [step libraries](step-libraries.md). Each step 
library is allowed to include a file named _dependencies.json_ in the root classpath if there are additional dependent
jars (including step libraries) that are needed. The basic structure appears as follows:

```json
{
  "<resolverType>": {
    "libraries": []
  }
}
```

Here is a working example:

```json
{
  "maven": {
    "libraries": [
      {
        "groupId": "org.mongodb.spark",
        "artifactId": "mongo-spark-connector_2.11",
        "version": "2.3.2"
      },
      {
        "groupId": "org.mongodb",
        "artifactId": "mongo-java-driver",
        "version": "3.11.2"
      }
    ]
  }
}
```

The resolver is set to _maven_ and each library entry provides a structure defined by the Maven dependency resolver. Once
the jars have been processed, a classpath will be printed to the console. All jars will be copied to the _output-path_.

## Running
Metalus Utils must be [installed](../metalus-utils/readme.md#installation) before running the script. 

The script parameters are:
* --jar-files - A comma separated list of jar files. This should be the full path. Paths starting with _http_ will attempt
to download the jars. The authorization parameters will be used if provided unless the _no-auth-download_ is provided.
* --no-auth-download - An optional parameter which when set to true will not use the authorization parameters when 
downloading jars.
* --output-path - A path to write the jar files.
* --path-prefix - An optional path prefix to add to each jar in the classpath
* --jar-separator - An optional separator character to use when building the classpath

**Authorization**:
When pushing metadata to an API or downloading jars, [authorization](httprestclient.md#authorization) is not used unless 
the authorization parameters are provided.

**Example commands**:

Generate the classpath:
```bash
bin/dependency-resolver.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --output-path /tmp
```

Generate the classpath with custom separator:
```bash
bin/dependency-resolver.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --output-path /tmp --jar-separator :
```

Generate the classpath overriding the maven repo:
```bash
bin/dependency-resolver.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --output-path /tmp --maven.repo http://localhost/maven2
```

Generate the classpath overriding the maven repo to use the local:
```bash
bin/dependency-resolver.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --output-path /tmp --maven.repo file://home/user/.m2/repository
```

Example Output with a _path-prefix_ of _hdfs://acxiom/jars/udl_:

```shell script
hdfs://acxiom/jars/udl/metalus-mongo_2.11-spark_2.3-1.6.0-SNAPSHOT.jar:hdfs://acxiom/jars/udl/mongo-spark-connector_2.11-2.3.2.jar:hdfs://acxiom/jars/udl/mongo-java-driver-3.11.2.jar
```

## Maven Dependency Resolver
Metalus ships with a custom _Maven_ dependency resolver. By default it will resolve dependencies using the central repo.
Developers may choose to override the repo within the _dependencies.json_ file by adding the _repo_ attribute like this:

```json
{
  "maven": {
    "repo": "http://localhost/maven2",
    "libraries": [
      {
        "groupId": "org.mongodb.spark",
        "artifactId": "mongo-spark-connector_2.11",
        "version": "2.3.2"
      },
      {
        "groupId": "org.mongodb",
        "artifactId": "mongo-java-driver",
        "version": "3.11.2"
      }
    ]
  }
}
```

Additionally, the repo may be overridden when calling the script by passing the following command line parameter:

```shell script
--maven.repo http://localhost/maven2
```

The local maven cache may also be used by passing in the command line parameter:
```shell script
--maven.repo file://home/user/.m2/repository
```

Multiple repos may be provided by separating the paths with a comma:
```shell script
--maven.repo http://localhost/maven2,file://home/user/.m2/repository
```
