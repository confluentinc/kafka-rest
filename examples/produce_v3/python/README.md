# Motivation for the example

The example code is in `streaming_produce_v3_main.py`, using the produce-v3 [streaming API](https://github.com/confluentinc/kafka-rest/blob/8fb324845b5a7bd48aba24cd6dfd6136534a0588/api/v3/openapi.yaml#L1416) of the REST proxy. Produce-records are written and record-receipts are read on a single HTTP connection with `Tranfer-encoding: chunked`([chunked encoding in rfc](https://datatracker.ietf.org/doc/html/rfc2616#section-3.6.1)). This script demonstrates how to be fully-duplex, i.e read record-receipts interleaved with records being written, on the same http-connection. For example, below output from a sample run of the script shows, interleaved reading and writing from the Http connection.

```
Writing a record #2 with json b'{"value": {"type": "STRING", "data": "value_1"}, "key": {"type": "STRING", "data": "key_1"}}'
Receipt for record #2 is ******
b'{"error_code":200,"cluster_id":"EV-5o5e3SViiGP0hpgKn1g","topic_name":"topic_1","partition_id":0,"offset":612,"timestamp":"2023-05-16T18:20:51.844Z","key":{"type":"STRING","size":5},"value":{"type":"STRING","size":7}}\r\n'
Writing a record #3 with json b'{"value": {"type": "STRING", "data": "value_2"}, "key": {"type": "STRING", "data": "key_2"}}'
Receipt for record #3 is ******
b'{"error_code":200,"cluster_id":"EV-5o5e3SViiGP0hpgKn1g","topic_name":"topic_1","partition_id":0,"offset":613,"timestamp":"2023-05-16T18:20:52.852Z","key":{"type":"STRING","size":5},"value":{"type":"STRING","size":7}}\r\n'
```

## Running the code

The code assumes there is a Kafka-rest proxy exposing http-interface( on "localhost", "8082" default port). Follow the instructions kafka-rest [repo](https://github.com/confluentinc/kafka-rest) to run the proxy.

Now run the main driver script:
`python3 streaming_produce_v3_main.py`

**This script will work with python-version >= 3.0**