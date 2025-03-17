#!/bin/bash

# Define the schema directly as a string. Ensure it is properly escaped.
value_schema='{"type": "record", "name": "UserRecord", "fields": [{"name": "name", "type": "string"}'
# Add 15 dummy fields to the schema string
for i in $(seq 1 17); do
  value_schema+=',{"name": "field'"$i"'", "type": "string"}'
done
value_schema+=']}'

# Encode the schema as a JSON string using jq by passing it as raw input (--raw-input / -R) and escaping it as a JSON string (--slurp / -s)
value_schema_json=$(echo "$value_schema" | jq -Rs .)

# Start building the records part of the payload
records_array='['
for j in $(seq 1 1000); do
  record='{"value": {"name": "testUser'"$j"'"'
  # Add dummy values for each of the 15 fields
  for k in $(seq 1 17); do
    record+=', "field'"$k"'": "value'"$k"'"'
  done
  record+='}}'
  # Add comma between records, not after the last record
  if [ $j -lt 1000 ]; then
    records_array+="$record,"
  else
    records_array+="$record"
  fi
done
records_array+=']'

# Combine value_schema and records into the final JSON payload
json_payload=$(jq -n \
                  --argjson schema "$value_schema_json" \
                  --argjson recs "$records_array" \
                  '{value_schema: $schema, records: $recs}')

# Use the generated JSON payload in the curl command
# Note: Depending on your shell and jq version, you might need to tweak the handling of variables and quotes
curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
     -H "Accept: application/vnd.kafka.v2+json" \
     --data "$json_payload" \
     "http://localhost:8082/topics/test-topic-19"
