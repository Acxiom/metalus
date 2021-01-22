#!/usr/bin/env bash

# Function to compare the results of the test data against the generated metadata.
checkResults() {
  project=$1
  file=$2
  stagingDir=`echo $tmpDir/staging/${project}*`
  baseFile=$dir/manual_tests/testData/$project/${file}.json
  compareFile=$stagingDir/${file}.json
  $dir/manual_tests/jq 'del(.. | .tags?)' $baseFile > $stagingDir/${file}_base.json
  $dir/manual_tests/jq 'del(.. | .tags?)' $stagingDir/${file}.json > $stagingDir/${file}_updated.json
  results=`diff --brief $stagingDir/${file}_base.json $stagingDir/${file}_updated.json`
  if [ ${#results} -gt 0 ]
  then
    echo "There was a difference found in the ${project} project for file ${file}!"
    exit 1
  fi
}

# Setup the temporary location for testing
bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir=$(dirname "${bindir}")
tmpDir="${dir}/manual_tests/tmp"
rm -rf $tmpDir
mkdir $tmpDir

# Copy the built Metalus Utils to a temporary directory
cp ${dir}/metalus-utils/target/*.gz $tmpDir
cd $tmpDir
tar xf $tmpDir/*.gz

# Get a list of jar files
jarFiles=`ls $dir/metalus-aws/target/metalus-aws*.jar | grep -v javadoc`
jarFiles+=","
jarFiles+=`ls $dir/metalus-gcp/target/metalus-gcp*.jar | grep -v javadoc`
jarFiles+=","
jarFiles+=`ls $dir/metalus-common/target/metalus-common*.jar | grep -v javadoc`
jarFiles+=","
jarFiles+=`ls $dir/metalus-kafka/target/metalus-kafka*.jar | grep -v javadoc`
jarFiles+=","
jarFiles+=`ls $dir/metalus-mongo/target/metalus-mongo*.jar | grep -v javadoc`

# Run the command to get the data
mkdir $tmpDir/staging
$tmpDir/metalus-utils/bin/metadata-extractor.sh \
--output-path $tmpDir/staging \
--jar-files $jarFiles \
--no-auth-download true \
--repo ~/.m2 \
--clean-staging true

# Get the jq command
if [[ ! -f $dir/manual_tests/jq ]]; then
  if [[ "$OSTYPE" == "linux-gnu"* ]]
  then
        curl -L https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 > $dir/manual_tests/jq
  fi
  if [[ "$OSTYPE" == "darwin"* ]]
  then
        curl -L https://github.com/stedolan/jq/releases/download/jq-1.6/jq-osx-amd64 > $dir/manual_tests/jq
  fi
fi
chmod +x $dir/manual_tests/jq

# Validate the results
checkResults "metalus-aws" "pipelines"
checkResults "metalus-aws" "steps"
checkResults "metalus-gcp" "pipelines"
checkResults "metalus-gcp" "steps"
checkResults "metalus-common" "pipelines"
checkResults "metalus-common" "steps"
checkResults "metalus-kafka" "pipelines"
checkResults "metalus-kafka" "steps"
checkResults "metalus-mongo" "pipelines"
checkResults "metalus-mongo" "steps"

# Copy the templates to another directory if required
if [[ -n ${1} && ${1} = true ]]
then
  mkdir -p $dir/manual_tests/metadata_templates
  mv $tmpDir/staging/* $dir/manual_tests/metadata_templates/
fi

# Cleanup the temp directory
rm -rf $tmpDir
