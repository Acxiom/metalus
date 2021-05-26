This directory contains manual tests that should be performed for each version prior to release.

Metalus Spark Tests
The test harness will build the code against a specific version combination. Once the build completes, the code is then
run on a local Spark server related to that version (except 2.4_2.12) and then metadata is extracted and compared.
Mongo must be installed locally and on the main path.

The tests may be run for all 4 supported versions or a single version using the --version command line parameter.

Versions:
* 2.4
* 2.4_2.12 (does not run manual tests)
* 3.0
* 3.1
* all (default)

base command:

./manual-tests.sh

command to save the extracted metadata:

./manual-tests.sh --save-metadata true

command to save the extracted metadata for a single version:

./manual-tests.sh --save-metadata true --version 3.1
