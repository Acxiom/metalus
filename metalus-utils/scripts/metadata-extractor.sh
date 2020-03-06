#!/usr/bin/env bash

usage()
{
	echo "metadata-extractor.sh [OPTIONS]"
	echo "--output-path   -> A path to write the JSON output. This parameter is optional."
	echo "--api-url       -> The base URL to use when pushing data to an API. This parameter is optional."
	echo "--api-path      -> The base path to use when pushing data to an API. This parameter is optional and defaults to /api/v1."
	echo "--jar-files     -> A comma separated list of jar files to scan"
}

authorization=""

# Parse the parameters
while [[ "$1" != "" ]]; do
    case $1 in
        --output-path )         shift
                                outputPath=$1
                                ;;
        --jar-files )           shift
                                jarFiles=$1
                                ;;
        --authorization.class ) shift
                                authorization="--authorization.class ${1}"
                                ;;
        --authorization.username ) shift
                                authorization="${authorization} --authorization.username ${1}"
                                ;;
        --authorization.password ) shift
                                authorization="${authorization} --authorization.password ${1}"
                                ;;
        --help )                usage
                                exit 1
                                ;;
        * )                     rootParams="${rootParams} ${1}"
                                shift
                                rootParams="${rootParams} ${1}"
                                ;;
    esac
    shift
done

bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir=$(dirname "${bindir}")

# Create the initial classPath
classPath=""
for i in "${dir}"/libraries/*.jar
do
    # Add to the classPath
    classPath="${classPath}:${i}"
done

# Add the provided jars to the classpath to make it easier to retrieve
for i in ${jarFiles//,/ /g}
do
    # Resolve the dependencies and add to the class path
    stagingDir="${dir}/staging"
    dependencies=$(exec $dir/bin/dependency-resolver.sh $authorization --output-path $stagingDir --jar-files $i --path-prefix $stagingDir)
    jarName=${i##*/}
    dirName=${jarName%.jar}
    params="--jar-files ${stagingDir}/${jarName} ${rootParams}"

    if [[ -n "${outputPath}" ]]
    then
        params="${params} --output-path ${outputPath}/${dirName}"
        mkdir -p "${outputPath}/${dirName}"
    fi

    extraClasspath=${dependencies//,/:/g}
    java -cp "${classPath}:${extraClasspath}" com.acxiom.metalus.MetadataExtractor $params $authorization
    ret=${?}

    if [[ $ret -ne 0 ]]
    then
      echo "Failed to extract metadata due to unhandled exception for jar: ${jarName}"
      exit $ret
    fi

    echo "${jarName} complete"
done