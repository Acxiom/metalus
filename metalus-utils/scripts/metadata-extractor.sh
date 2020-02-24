#!/usr/bin/env bash

usage()
{
	echo "metadata-extractor.sh [OPTIONS]"
	echo "--output-path   -> A path to write the JSON output. This parameter is optional."
	echo "--api-url       -> The base URL to use when pushing data to an API. This parameter is optional."
	echo "--api-path      -> The base path to use when pushing data to an API. This parameter is optional and defaults to /api/v1."
	echo "--jar-files     -> A comma separated list of jar files to scan"
}

# Parse the parameters
while [[ "$1" != "" ]]; do
    case $1 in
        --output-path )         shift
                                outputPath=$1
                                ;;
        --jar-files )           shift
                                jarFiles=$1
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

bindir=$(cd `dirname ${BASH_SOURCE[0]}` && pwd)
dir=$(dirname "${bindir}")

# Create the initial classPath
classPath=""
for i in $(ls ${dir}/libraries)
do
    # Add to the classPath
    classPath="${classPath}:${dir}/libraries/${i}"
done

# Add the provided jars to the classpath to make it easier to retrieve
for i in $(echo ${jarFiles} | sed "s/,/ /g")
do
    # Resolve the dependencies and add to the class path
    stagingDir="${dir}/staging"
    dependencies=`exec ${dir}/bin/dependency-resolver.sh --output-path $stagingDir --jar-files ${i} --path-prefix $stagingDir`
    params="--jar-files ${dependencies} ${rootParams}"
    jarName=${i##*/}
    dirName=${jarName%.jar}

    if [[ -n "${outputPath}" ]]
    then
        params="${params} --output-path ${outputPath}/${dirName}"
        mkdir -p "${outputPath}/${dirName}"
    fi

    extraClasspath=$(echo ${dependencies} | sed "s/,/:/g")
    java -cp "${classPath}:${extraClasspath}" com.acxiom.metalus.MetadataExtractor $params

    echo "${jarName} complete"
done
