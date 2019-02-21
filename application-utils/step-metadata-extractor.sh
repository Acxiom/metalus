#!/usr/bin/env bash

usage()
{
	echo "step-metadata-extractor.sh [OPTIONS]"
	echo "--step-packages -> A comma separated list of packages to scan"
	echo "--output-file   -> A file name to write the JSON output. This parameter is optional."
	echo "--jar-files     -> A comma separated list of jar files to scan"
}

# Parse the parameters
while [[ "$1" != "" ]]; do
    case $1 in
        --step-packages )       shift
                                stepPackages=$1
                                ;;
        --output-file )    		shift
        						outputFile=$1
                                ;;
        --jar-files )           shift
        						jarFiles=$1
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

script=${BASH_SOURCE[0]}
bindir=$(cd `dirname ${script}` && pwd)
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
    # Add to the classPath
    classPath="${classPath}:${i}"
done

params="--step-packages ${stepPackages} --jar-files ${jarFiles}"

if [[ -n "${outputFile}" ]]
then
	params="${params} --output-file ${outputFile}"
fi

exec scala -cp ${classPath} com.acxiom.pipeline.annotations.StepMetaDataExtractor ${params}
