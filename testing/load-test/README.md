# Load Tests

To execute the load tests included here, you need to [install k6](
https://k6.io/docs/getting-started/installation).

Then you can run any test by executing:

```shell script
$ k6 run <my-test-script.js>
```

For example:

```shell script
$ k6 run v3/get-broker-test.js
```

All tests will execute against a REST Proxy instance running on `http://localhost:9391` (this is
defined in `v2/common.js` and `v3/common.js`. Before running a test, make sure you have a REST Proxy
server up and running. For a quick way of getting a development server up, see the [minimal test
environment](
https://github.com/confluentinc/kafka-rest/blob/master/testing/environments/minimal/README.md).

## Experimental

You can use the `v2/run.sh` and `v3/run.sh` run scripts to run all the load tests in each folder. A
test environment will be started for each test. The individual test summaries will be available at
`v?/target/${test_name}.out` in JSON format, and a summary of all results will be available at
`v?/target/summary.xlsx` as a spreadsheet.
