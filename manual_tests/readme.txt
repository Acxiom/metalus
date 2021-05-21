This directory contains manual tests that should be performed for each version prior to release.

Metalus Spark Tests
These tests will run the metadata extractor as well the example application against Spark 2.4 and 3.0. Mongo must be
running locally, but Spark runtimes will be pulled and executed.

base command:

./manual-tests.sh

command to save the extracted metadata:

./manual-tests.sh true
