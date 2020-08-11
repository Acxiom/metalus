#!/usr/bin/env bash

usage() {
  echo "dependency-resolver.sh [OPTIONS]"
  echo "--output-path             -> A path to write the jars"
  echo "--jar-files               -> A comma separated list of jar files to scan"
  echo "--jar-separator           -> A single character that will be used to separate the jars when building the classpath"
  echo "--path-prefix             -> The path to prepend to the jar files"
  echo "--include-scopes          -> Optional comma separated list of scopes to include. runtime is always included."
  echo "--allow-self-signed-certs -> Boolean flag enabling self signed certificates"
}

# Parse the parameters
while [[ "$1" != "" ]]; do
  case $1 in
  --help)
    usage
    exit 1
    ;;
  --allow-self-signed-certs) shift
                            params="${params} --allowSelfSignedCerts ${1}"
                            ;;
  *)
    params="${params} ${1}"
    shift
    params="${params} ${1}"
    ;;
  esac
  shift
done

bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir=$(dirname "${bindir}")

# Create the classPath
classPath=""
for i in "${dir}"/libraries/*.jar
do
  # Add to the classPath
  classPath="${classPath}:${i}"
done

java -cp $classPath com.acxiom.metalus.DependencyManager $params
ret=${?}

if [[ $ret -ne 0 ]]; then
  echo "Failed to resolve dependencies due to unhandled exception."
  exit $ret
fi
