# Example code to run produce-v3 streaming API

The example code is in `streaming_produce_v3_main.py`, using the produce-v3 [streaming API](https://github.com/confluentinc/kafka-rest/blob/8fb324845b5a7bd48aba24cd6dfd6136534a0588/api/v3/openapi.yaml#L1416) of the REST proxy. This produces records and reads their receipts on a single HTTP connection in full duplex fashion.

## Requirements

Install requirements:

`pip install -r requirements.txt`

Set PYTHONPATH:
`export PYTHONPATH=PYTHONPATH:${<path to python directory under kafka-rest>}`

## Running the script

Now run the main driver script:
`python3 streaming_produce_v3_main.py`

**This scipt will work with python-version >= 3.0**

## Code Structure

Produce-records are written and record-receipts are read on a single HTTP connection with `Tranfer-encoding: chunked`([chunked encoding in rfc](https://datatracker.ietf.org/doc/html/rfc2616#section-3.6.1)). In order to be fully duplex, this implements `http_parser_chunked.HttpResponse`. This provides an iterator interface and allows the each individual chunk, of the http-reponse-message-body, to be read(& processed) separately.

`http_parser_chunked` module utilises [http-parser](https://github.com/benoitc/http-parser/tree/master)(under the MIT license). The relevant code is pulled into sub-directory `http_parser_chunked/http_parser`.

## Developing

Note there are some tests in `tests/` directory to test high-level functionality of the parsers. To run them go -
`pytest`