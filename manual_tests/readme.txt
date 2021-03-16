This directory contains manual tests that should be performed for each version prior to release.

Metalus Spark Tests
This test requires that the maven build is executed for the specific version prior to running the test. A
smoke test running against different versions of Spark to ensure that the compiled jars will run as expected.

Define the test:
	* Download the specific versions of Spark if they don't already exist
	* Use the application example
	* Start the slave node
	* Start the master node
	* Run the Spark submit command against the jars in the target directories
	* Connect to mongo and verify the results
	* Drop database
Is there a way to automatically run these tests?

Metalus Extraction Tests
This test requires that the maven build is executed for the specific version prior to running the test. The
local maven repo "~/.m2" will be used to pull dependent artifacts. Each build needs to be run prior too
running the manual tests.


Scala 2.11 Spark 2.4
Build: mvn clean install
Tests:
manual_tests/metadata-extractor-test.sh
manual_tests/spark-test.sh

Scala 2.12 Spark 3.0
Build: mvn -P spark_3.0 clean install
Tests:
manual_tests/metadata-extractor-test.sh
manual_tests/spark-test.sh
