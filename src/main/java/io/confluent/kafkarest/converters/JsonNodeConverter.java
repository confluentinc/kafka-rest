package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;

interface JsonNodeConverter {
    JsonNode convert(JsonNode jsonNode, Schema schema);
}
