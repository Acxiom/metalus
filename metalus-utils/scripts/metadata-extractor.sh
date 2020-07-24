#!/usr/bin/env bash

usage()
{
	echo "metadata-extractor.sh [OPTIONS]"
	echo "--output-path            -> A path to write the JSON output. This parameter is optional."
	echo "--api-url                 -> The base URL to use when pushing data to an API. This parameter is optional."
	echo "--api-path                -> The base path to use when pushing data to an API. This parameter is optional and defaults to /api/v1."
	echo "--jar-files               -> A comma separated list of jar files to scan"
	echo "--repo                    -> An optional comma separated list of repositories to scan for dependencies"
	echo "--staging-dir             -> An optional directory path to stage jars"
	echo "--clean-staging           -> Indicates whether the staging directory should be cleaned"
	echo "--allow-self-signed-certs -> Boolean flag enabling self signed certificates"
}

authorization=""
cleanStaging=false

# Parse the parameters
while [[ "$1" != "" ]]; do
    case $1 in
        --output-path )         shift
                                outputPath=$1
                                ;;
        --jar-files )           shift
                                jarFiles=$1
                                ;;
        --allow-self-signed-cert) shift
                                  allowSelfSignedCerts=$1
                                  rootParams="${rootParams} --allowSelfSignedCerts ${allowSelfSignedCerts}"

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
        --no-auth-download)     shift
                                noAuthDownload=true
                                authorization="${authorization} --no-auth-download ${1}"
                                ;;
        --staging-dir)          shift
                                stagingDirectory=$1
                                ;;
        --clean-staging)        shift
                                cleanStaging=$1
                                ;;
        --repo)                 shift
                                repo="--repo ${1}"
                                ;;
        --extra-classpath)      shift
                                extraCP="${1}"
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

if [[ -n "${extraCP}" ]]
then
  classPath="${classPath}:${extraCP}"
fi

downloadAuth=$authorization
if [[ -n "${noAuthDownload}" ]]
then
  downloadAuth=""
fi

if [[ -z "${allowSelfSignedCerts}" ]]
then
  allowSelfSignedCerts=false
fi

# Resolve the dependencies and add to the class path
stagingDir="${dir}/staging"
if [[ -n "${stagingDirectory}" ]]
then
  stagingDir="${stagingDirectory}"
fi

# Clean the staging directory before starting
if [ "${cleanStaging}" = true ]
then
  rm -f ${stagingDir:?}/*.jar
fi

# Add the provided jars to the classpath to make it easier to retrieve
for i in ${jarFiles//,/ }
do
    dependencies=$(exec $dir/bin/dependency-resolver.sh --include-scopes extraction --allowSelfSignedCerts $allowSelfSignedCerts --output-path $stagingDir --jar-files $i --path-prefix $stagingDir $downloadAuth $repo)
    dependencies=$(echo "${dependencies}" | tail -n1)
    jarName=${i##*/}
    dirName=${jarName%.jar}
    params="--jar-files ${stagingDir}/${jarName} ${rootParams}"

    if [[ -n "${outputPath}" ]]
    then
        params="${params} --output-path ${outputPath}/${dirName}"
        mkdir -p "${outputPath}/${dirName}"
    fi

    extraClasspath=${dependencies//,/:}
    java -cp "${classPath}:${extraClasspath}" com.acxiom.metalus.MetadataExtractor $params $authorization
    ret=${?}

    if [[ $ret -ne 0 ]]
    then
      echo "Failed to extract metadata due to unhandled exception for jar: ${jarName}"

      # Clean the staging directory
      if $cleanStaging
      then
        rm -rf "${stagingDir:?}/*"
      fi

      exit $ret
    fi

    echo "${jarName} complete"
done

# Clean the staging directory
if [ "${cleanStaging}" = true ]
then
  rm -f ${stagingDir:?}/*.jar
fi
