#!/usr/bin/env bash

validateResult() {
  ret=$1
  msg=$2
  if [[ $ret -ne 0 ]]; then
    echo $msg
    exit $ret
  fi
}

usage() {
  echo "manual-tests.sh [OPTIONS]"
  echo "--save-metadata          -> When true, all metadata generated during the test will be saved to the metadata_templates directory"
  echo "--version                 -> The specific version to test. Allowed versions are: 2.4, 2.4_2.12, 3.0, 3.1, 3.2 and 3.3. Defaults to 'all'"
}

buildVersion="all"
# Parse the parameters
while [[ "$1" != "" ]]; do
  case $1 in
  --help)
    usage
    exit 1
    ;;
  --save-metadata) shift
                    storeMetadata="${1}"
                    ;;
  --version) shift
                    buildVersion="${1}"
                    ;;
  *)
    usage
    exit 1
    ;;
  esac
  shift
done

# Setup the temporary location for testing
bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir=$(dirname "${bindir}")
cd "$dir" || exit
echo "Executing from ${dir}"
# Remove snapshot so the metadata uses clean tags
mvn -P spark_3.1 -B versions:set -DremoveSnapshot

# 2.4
if [[ "${buildVersion}" == "2.4" || "${buildVersion}" == "all" ]]
then
  echo "Testing Spark 2.4"
  mvn -P spark_2.4,scala_2.11 clean install
  validateResult ${?} "Failed to build project"
  manual_tests/spark-test.sh
  validateResult ${?} "Failed Spark Test"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
fi

# 2.4 Scala 2.12
if [[ "${buildVersion}" == "2.4_2.12" || "${buildVersion}" == "all" ]]
then
  echo "Testing Spark 2.4 Scala 2.12"
  mvn -P spark_2.4,scala_2.12 clean install
  validateResult ${?} "Failed to build project"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
fi

# 3.0
if [[ "${buildVersion}" == "3.0" || "${buildVersion}" == "all" ]]
then
  echo "Testing Spark 3.0"
  mvn -P spark_3.0 clean install
  validateResult ${?} "Failed to build project"
  manual_tests/spark-test.sh
  validateResult ${?} "Failed Spark Test"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
fi

# 3.1
if [[ "${buildVersion}" == "3.1" || "${buildVersion}" == "all" ]]
then
  echo "Testing Spark 3.1"
  mvn -P spark_3.1 clean install
  validateResult ${?} "Failed to build project"
  manual_tests/spark-test.sh
  validateResult ${?} "Failed Spark Test"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
fi

# 3.2
if [[ "${buildVersion}" == "3.2" || "${buildVersion}" == "all" ]]
then
  echo "Testing Spark 3.2"
  mvn -P spark_3.2 clean install
  validateResult ${?} "Failed to build project"
  manual_tests/spark-test.sh
  validateResult ${?} "Failed Spark Test"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
fi

# 3.3
if [[ "${buildVersion}" == "3.3" || "${buildVersion}" == "all" ]]
then
  echo "Testing Spark 3.3"
  mvn -P spark_3.2 clean install
  validateResult ${?} "Failed to build project"
  manual_tests/spark-test.sh
  validateResult ${?} "Failed Spark Test"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
fi

# 3.2  Scala 2.13 TODO Some libraries do not support scala 2.13 like scalamock
#if [[ "${buildVersion}" == "3.2_2.13" || "${buildVersion}" == "all" ]]
#then
#  echo "Testing Spark 3.2"
#  mvn -P spark_3.2,scala_2.13 clean install
#  validateResult ${?} "Failed to build project"
#  manual_tests/spark-test.sh
#  validateResult ${?} "Failed Spark Test"
#  manual_tests/metadata-extractor-test.sh $storeMetadata
#  validateResult ${?} "Failed Metadata Extractor Test"
#fi

# Set the version back to the original
version=`mvn -P spark_3.1 -q -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec`
mvn -P spark_3.1 versions:set -DnewVersion="${version}-SNAPSHOT"
