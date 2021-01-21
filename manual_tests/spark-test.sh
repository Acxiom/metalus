#!/usr/bin/env bash

# Setup the temporary location for testing
bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir=$(dirname "${bindir}")
tmpDir="${dir}/manual_tests/tmp"
serversDir="${dir}/manual_tests/servers"
rm -rf $tmpDir
mkdir $tmpDir
mkdir -p $serversDir

# Determine Scala/Spark Version
regex='(metalus-aws_)([^-]*)'
sparkRegex='(spark_)([^-]*)'
jarFiles=`ls $dir/metalus-aws/target/metalus-aws*.jar | grep -v javadoc`
[[ $jarFiles =~ $regex ]]
scalaCompat=${BASH_REMATCH[2]}
[[ $jarFiles =~ $sparkRegex ]]
sparkCompat=${BASH_REMATCH[2]}
# Download/Unpack Spark
cd $serversDir
jarFiles=""
if [[ "$sparkCompat" == "2.4" ]]
then
  if [[ ! -f $serversDir/spark-2.4.7-bin-hadoop2.7.tgz ]]
  then
    echo "Downloading 2.4 Spark"
    curl -L https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz > $serversDir/spark-2.4.7-bin-hadoop2.7.tgz
    curl -L https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.11/2.4.2/mongo-spark-connector_2.11-2.4.2.jar > $serversDir/mongo-spark-connector_2.11-2.4.2.jar
    curl -L https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.7/mongo-java-driver-3.12.7.jar > $serversDir/mongo-java-driver-3.12.7.jar
    tar xf $serversDir/spark-2.4.7-bin-hadoop2.7.tgz
  fi
  sparkDir="${serversDir}/spark-2.4.7-bin-hadoop2.7"
  jarFiles="${serversDir}/mongo-spark-connector_2.11-2.4.2.jar,${serversDir}/mongo-java-driver-3.12.7.jar,"
fi
if [[ "$sparkCompat" == "3.0" ]]
then
  if [[ ! -f $serversDir/spark-3.0.1-bin-hadoop2.7.tgz ]]
  then
    echo "Downloading 3.0 Spark"
    curl -L https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz > $serversDir/spark-3.0.1-bin-hadoop2.7.tgz
    curl -L https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.0/mongo-spark-connector_2.12-3.0.0.jar > $serversDir/mongo-spark-connector_2.12-3.0.0.jar
    curl -L https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.0.5/mongodb-driver-core-4.0.5.jar > $serversDir/mongodb-driver-core-4.0.5.jar
    curl -L https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.0.5/mongodb-driver-sync-4.0.5.jar > $serversDir/mongodb-driver-sync-4.0.5.jar
    curl -L https://repo1.maven.org/maven2/org/mongodb/bson/4.0.5/bson-4.0.5.jar > $serversDir/bson-4.0.5.jar
    tar xf $serversDir/spark-3.0.1-bin-hadoop2.7.tgz
  fi
  sparkDir="${serversDir}/spark-3.0.1-bin-hadoop2.7"
  jarFiles="${serversDir}/mongo-spark-connector_2.12-3.0.0.jar,${serversDir}/mongodb-driver-sync-4.0.5.jar,${serversDir}/mongodb-driver-core-4.0.5.jar,${serversDir}/bson-4.0.5.jar,"
fi
cd ..
# Startup Mongo
mkdir -p $tmpDir/data
mkdir -p $tmpDir/mongodb
mongod --fork --logpath $tmpDir/mongodb/mongod.log --dbpath $tmpDir/data
# Start Spark
export SPARK_LOCAL_IP=127.0.0.1
$sparkDir/sbin/start-master.sh -h localhost -p 7077
$sparkDir/sbin/start-slave.sh localhost:7077 -h localhost
# Build classpath
jarFiles+=`ls $dir/metalus-common/target/metalus-common*.jar | grep -v javadoc`
jarFiles+=","
jarFiles+=`ls $dir/metalus-examples/target/metalus-examples*.jar | grep -v javadoc`
jarFiles+=","
jarFiles+=`ls $dir/metalus-mongo/target/metalus-mongo*.jar | grep -v javadoc`

# Run Spark Submit
applicationJar=`ls $dir/metalus-application/target/metalus-application*.jar | grep -v javadoc`
echo $applicationJar
$sparkDir/bin/spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars $jarFiles \
$applicationJar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath $dir/metalus-examples/mock_data/application-example.json \
--input_url $dir/metalus-examples/mock_data/orders.csv \
--input_format csv \
--mongoURI mongodb://localhost:27017/application_examples \
--validateStepParameterTypes true \
--logLevel DEBUG \
--input_separator ,

# Validate Results (use mongo shell)
mongoResults=`mongo < $dir/manual_tests/testData/validate_mongo_data.js | grep "count is not correct!"`

# Shutdown Spark
$sparkDir/sbin/stop-slave.sh
$sparkDir/sbin/stop-master.sh

# Shutdown Mongo
mongod < $dir/manual_tests/testData/stop_server.js

# Cleanup the temp directory
rm -rf $tmpDir

if [ ${#mongoResults} -gt 0 ]
then
  echo "Mongo data is not correct: ${mongoResults}"
  exit 1
fi
