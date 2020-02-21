#!/usr/bin/env bash

usage()
{
	echo "dependency-resolver.sh [OPTIONS]"
	echo "--output-path   -> A path to write the jars"
	echo "--jar-files     -> A comma separated list of jar files to scan"
	echo "--jar-separator -> A single character that will be used to separate the jars when building the classpath"
	echo "--path-prefix   -> The path to prepend to the jar files"
}

# Parse the parameters
while [[ "$1" != "" ]]; do
    case $1 in
        --output-path )    		shift
        						outputPath=$1
        						params="${params} --output-path ${outputPath}"
                                ;;
        --jar-files )           shift
        						jarFiles=$1
        						params="${params} --jar-files ${jarFiles}"
                                ;;
        --jar-separator )           shift
        						jarSeparator=$1
        						params="${params} --jar-separator ${jarSeparator}"
                                ;;
        --path-prefix )           shift
        						pathPrefix=$1
        						params="${params} --path-prefix ${pathPrefix}"
                                ;;
        --help )                usage
                                exit 1
    esac
    shift
done

script=${BASH_SOURCE[0]}
bindir=$(cd `dirname ${script}` && pwd)
dir=$(dirname "${bindir}")

# Create the classPath
classPath=""
for i in $(ls ${dir}/libraries)
do
    # Add to the classPath
    classPath="${classPath}:${dir}/libraries/${i}"
done

java -cp $classPath com.acxiom.metalus.DependencyManager $params
