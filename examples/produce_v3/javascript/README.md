# Motivation for the example

The example code is in `main.js`, using the produce-v3 [streaming API](https://github.com/confluentinc/kafka-rest/blob/8fb324845b5a7bd48aba24cd6dfd6136534a0588/api/v3/openapi.yaml#L1416) of the REST proxy. 
Produce-records are written and record-receipts are read on a single HTTP connection with `Transfer-encoding: chunked`([chunked encoding in rfc](https://datatracker.ietf.org/doc/html/rfc2616#section-3.6.1)). 
This script demonstrates how to be fully-duplex, i.e read record-receipts interleaved with records being written, on the same http-connection. 
For example, below output from a sample run of the script shows, interleaved reading and writing from the Http connection.

```shell
Producing record #7 with json {"value":{"type":"JSON","data":{"foo":7}}}
Receipt for record #7 is
{
  error_code: 200,
  cluster_id: 'ZXBWzl8VQ5WTB_hgEAuGeQ',
  topic_name: 'topic_1',
  partition_id: 0,
  offset: 158,
  timestamp: '2023-09-14T09:54:53.162Z',
  value: { type: 'JSON', size: 9 }
}
Producing record #8 with json {"value":{"type":"JSON","data":{"foo":8}}}
Receipt for record #8 is
{
  error_code: 200,
  cluster_id: 'ZXBWzl8VQ5WTB_hgEAuGeQ',
  topic_name: 'topic_1',
  partition_id: 0,
  offset: 159,
  timestamp: '2023-09-14T09:54:54.162Z',
  value: { type: 'JSON', size: 9 }
}
```

## Running the code

The code assumes there is a Kafka-rest proxy exposing http-interface (on "localhost", "8082" default port). 
Follow the instructions kafka-rest [repo](https://github.com/confluentinc/kafka-rest) to run the proxy.

Once you have [Node.js](https://nodejs.org/) installed, you can run the main driver script:
```shell
node main.js
```
