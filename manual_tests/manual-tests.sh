#!/usr/bin/env bash

validateResult() {
  ret=$1
  msg=$2
  if [[ $ret -ne 0 ]]; then
    echo $msg
    exit $ret
  fi
}

# Setup the temporary location for testing
bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir=$(dirname "${bindir}")
cd "$dir" || exit
echo "Executing from ${dir}"
# Remove snapshot so the metadata uses clean tags
mvn -B versions:set -DremoveSnapshot

# 2.4
echo "Testing Spark 2.4"
mvn clean install
validateResult ${?} "Failed to build project"
manual_tests/spark-test.sh
validateResult ${?} "Failed Spark Test"
manual_tests/metadata-extractor-test.sh $1
validateResult ${?} "Failed Metadata Extractor Test"

# 3.0
echo "Testing Spark 3.0"
mvn -P spark_3.0 clean install
validateResult ${?} "Failed to build project"
manual_tests/spark-test.sh
validateResult ${?} "Failed Spark Test"
manual_tests/metadata-extractor-test.sh $1
validateResult ${?} "Failed Metadata Extractor Test"

# 3.1
echo "Testing Spark 3.1"
mvn -P spark_3.1 clean install
validateResult ${?} "Failed to build project"
manual_tests/spark-test.sh
validateResult ${?} "Failed Spark Test"
manual_tests/metadata-extractor-test.sh $1
validateResult ${?} "Failed Metadata Extractor Test"

# Set the version back to the original
version=`mvn -q -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec`
mvn versions:set -DnewVersion="${version}-SNAPSHOT"
