package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Objects;

public class JsonNodeConverters {

    static JsonNode textToDecimal(JsonNode jsonNode, Schema schema) {
        if (jsonNode instanceof TextNode){
            String encodedBigDecimal = jsonNode.asText();
            int scale = schema.getJsonProps().get("scale").asInt();
            BigDecimal bd = BigDecimalDecoder.fromEncodedString(encodedBigDecimal, scale);
            return new DecimalNode(bd);
        } else {
            return jsonNode;
        }
    };

    static JsonNode decimalToText(JsonNode jsonNode, Schema schema) {
        if (jsonNode instanceof DoubleNode) {
            BigDecimal bd = jsonNode.decimalValue();
            int scale = schema.getJsonProps().get("scale").asInt();
            int precision = schema.getJsonProps().get("precision").asInt();
            String bdBytesAsUtf8 = BigDecimalEncoder.toEncodedString(bd, scale, precision);
            return new TextNode(bdBytesAsUtf8);
        } else {
            return jsonNode;
        }
    }


    private void dummy() {
        transformJsonNode(null, null, new JsonNodeConverter() {
            @Override
            public JsonNode convert(JsonNode jsonNode, Schema schema) {
                return textToDecimal(jsonNode, schema);
            }
        });
    }

    //  private static JsonNode transformJsonNode(JsonNode jsonNode, Schema schema) {
    static JsonNode transformJsonNode(JsonNode jsonNode, Schema schema, JsonNodeConverter jsonNodeConverter) {
        if (schema.getType() == Schema.Type.BYTES && schema.getJsonProps().containsKey("logicalType") &&
                Objects.equals(schema.getJsonProps().get("logicalType").asText(), "decimal"))  {
            return jsonNodeConverter.convert(jsonNode, schema);
        } else if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field s : schema.getFields()){
                JsonNode transformed = transformJsonNode(jsonNode.get(s.name()), s.schema(), jsonNodeConverter);
                ((ObjectNode) jsonNode).set(s.name(), transformed);
            }
        } else if (schema.getType() == Schema.Type.UNION) {
            if (jsonNode.has("bytes") && jsonNode.get("bytes").isNumber()) {
                for (Schema subSchema : schema.getTypes()) {
                    if (subSchema.getType() == Schema.Type.BYTES && subSchema.getJsonProps().containsKey("logicalType") &&
                            Objects.equals(subSchema.getJsonProps().get("logicalType").asText(), "decimal")){
                        JsonNode transformed = transformJsonNode(jsonNode.get("bytes"), subSchema, jsonNodeConverter);
                        ((ObjectNode) jsonNode).set("bytes", transformed);
                    }
                }
            }
        } else if (schema.getType() == Schema.Type.ARRAY) {
            Schema subSchema = schema.getElementType();
            int i = 0;
            for (Iterator<JsonNode> it = jsonNode.elements(); it.hasNext(); ) {
                JsonNode elem = it.next();
                JsonNode transformed = transformJsonNode(elem, subSchema, jsonNodeConverter);
                ((ArrayNode) jsonNode).set(i, transformed);
                i += 1;
            }
        }
        // TODO: More cases - missing MAP and ENUM
        return jsonNode;
    }
}
