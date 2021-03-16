[Documentation Home](readme.md)

# Metalus Application
This project provides a single self contained jar containing the metalus core and some third party libraries required to 
run a basic application.

## Submit a job locally:

### Start a local Spark:
Open a terminal window and change to the local Spark directory. **Note**: the version of Spark dictates the version of the
jars that need to be used.

**Scala 2.11 Spark 2.4**: metalus-application_2.11-spark_2.4-<VERSION>.jar
**Scala 2.12 Spark 3.0**: metalus-application_2.12-spark_3.0-<VERSION>.jar

Once the master and worker has been started, the UI may be accessed using these URLs:

**Master**: http://localhost:8080/
**Worker**: http://localhost:8081/

#### Start the master:

```bash
sbin/start-master.sh -h localhost -p 7077
```

#### Start the worker:

```bash
sbin/start-slave.sh localhost:7077 -h localhost
```


#### Stop the worker

```bash
sbin/stop-slave.sh
```

#### Stop the master

```bash
sbin/stop-master.sh
```

### Run the spark-submit command for Spark 2.4:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars <extra step jars> \
<jar_path>/metalus-application_2.11-spark_2.4-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <location of application-example.json> \
--logLevel DEBUG
`

### Run the spark-submit command for Spark 2.4:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars <extra step jars> \
<jar_path>/metalus-application_2.12-spark_3.0-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <location of application-example.json> \
--logLevel DEBUG
```
