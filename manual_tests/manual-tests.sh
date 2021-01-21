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
cd "$dir"

# 2.4
echo "Testing Spark 2.4"
mvn clean install
validateResult ${?} "Failed to build project"
manual_tests/metadata-extractor-test.sh
validateResult ${?} "Failed Metadata Extractor Test"
manual_tests/spark-test.sh
validateResult ${?} "Failed Spark Test"

# 3.0
echo "Testing Spark 3.0"
mvn -P spark_3.0 clean install
validateResult ${?} "Failed to build project"
manual_tests/metadata-extractor-test.sh
validateResult ${?} "Failed Metadata Extractor Test"
manual_tests/spark-test.sh
validateResult ${?} "Failed Spark Test"
