/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.kafkarest.entities.EntityUtils;

/**
 * Provides conversion of JSON to/from Avro.
 */
public class AvroConverter {

  public static Object toAvro(JsonNode value, Schema schema) {
    if (value == null) {
      return null;
    }

    switch (schema.getType()) {
      case ARRAY:
        if (!value.isArray()) {
          throw new ConversionException("Found non-array where schema specified an array.");
        }
        Schema elemSchema = schema.getElementType();
        GenericArray arrayResult = new GenericData.Array(value.size(), schema);
        for (JsonNode elem : value) {
          arrayResult.add(toAvro(elem, elemSchema));
        }
        return arrayResult;

      case BOOLEAN:
        if (!value.isBoolean()) {
          throw new ConversionException("Found non-boolean value where schema specified boolean.");
        }
        return value.asBoolean();

      case BYTES:
      case FIXED: // fixed also has size restrictions, but they're checked at serialization
        // We expect bytes to be represented as base64-encoded strings
        if (!value.isTextual()) {
          throw new ConversionException("Found non-bytes value where schema specified bytes.");
        }
        try {
          return EntityUtils.parseBase64Binary(value.asText());
        } catch (IllegalArgumentException e) {
          throw new ConversionException("Couldn't decode base64 encoded bytes.");
        }

      case DOUBLE:
        if (!value.isNumber()) {
          throw new ConversionException("Found non-float value where schema specified float.");
        }
        return value.asDouble();

      case ENUM:
        if (!value.isTextual()) {
          throw new ConversionException("Found non-string value where schema specified enum.");
        }
        String jsonSymbol = value.asText();
        return new GenericData.EnumSymbol(schema, jsonSymbol);

      case FLOAT:
        // There's no checking of the range of values like there is with integer types, so we
        // accept any number node, get it into double form, then check the range ourselves.
        if (!value.isNumber()) {
          throw new ConversionException("Found non-float value where schema specified float.");
        }
        double doubleValue = value.asDouble();
        if (doubleValue <= Float.MIN_VALUE || doubleValue >= Float.MAX_VALUE) {
          throw new ConversionException("Value of float field is outside valid range.");
        }
        return (float) doubleValue;

      case INT:
        if (!value.isInt()) {
          throw new ConversionException("Found non-int value where schema specified int.");
        }
        return value.asInt();

      case LONG:
        // JsonNode specifies exactly one type, so if it fits within an int isLong() will return
        // false
        if (!value.isLong() && !value.isInt()) {
          throw new ConversionException("Found non-long value where schema specified long.");
        }
        return value.asLong();

      case MAP:
        if (!value.isObject()) {
          throw new ConversionException("Found non-object where schema specified a map.");
        }
        Schema valueSchema = schema.getValueType();
        Map<String, Object> mapResult = new HashMap<String, Object>();
        for (Iterator<Map.Entry<String, JsonNode>> it = value.fields(); it.hasNext(); ) {
          Map.Entry<String, JsonNode> elem = it.next();
          mapResult.put(elem.getKey(), toAvro(elem.getValue(), valueSchema));
        }
        return mapResult;

      case NULL:
        if (!value.isNull()) {
          throw new ConversionException("Found non-null value where schema specified null.");
        }
        return null;

      case RECORD:
        if (!value.isObject()) {
          throw new ConversionException("Found non-object where schema specified record.");
        }
        GenericRecord recordResult = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
          // Find the value, checking the named field, aliases, and defaults.
          JsonNode fieldValue = null;
          String fieldName = field.name();
          if (value.has(fieldName)) {
            fieldValue = value.get(fieldName);
          } else {
            for (String alias : field.aliases()) {
              if (value.has(alias)) {
                fieldValue = value.get(alias);
                break;
              }
            }
          }

          if (fieldValue == null && field.defaultValue() == null) {
            throw new ConversionException("Missing default value.");
          }

          recordResult.put(fieldName, toAvro(fieldValue, field.schema()));
        }

        return recordResult;

      case STRING:
        if (!value.isTextual()) {
          throw new ConversionException("Found non-string value where schema specified string.");
        }
        return value.textValue();

      case UNION:
        // We could probably inline some checks to make this process more efficient, but this
        // works as an initial implementation
        for (Schema unionSchema : schema.getTypes()) {
          try {
            return toAvro(value, unionSchema);
          } catch (ConversionException e) {
            // ignore and continue trying
          }
        }
        throw new ConversionException("Couldn't convert to any of the union schema types");

      default:
        throw new ConversionException("Unsupported schema type.");
    }
  }

  /**
   * Converts Avro data (including primitive types) to their equivalent JsonNode representation.
   *
   * @param value the value to convert
   * @param size  (out) the approximate size of the JSON data when serialized. Since this method is
   *              called recursively, this out parameter should be passed in initialized to zero and
   *              the single instance is shared by all recursive calls
   * @return the root JsonNode representing the converted object
   */
  public static JsonNode toJson(Object value, AtomicInteger size) {
    if (value == null) {
      size.addAndGet(4);
      return JsonNodeFactory.instance.nullNode();
    }

    if (value instanceof Boolean) {
      size.addAndGet(5);
      return JsonNodeFactory.instance.booleanNode((Boolean) value);
    } else if (value instanceof Integer) {
      // All numbers use the average number of digits expected for their size. This is just an
      // approximation, so it's ok if these aren't entirely accurate
      size.addAndGet(3);
      return JsonNodeFactory.instance.numberNode((Integer) value);
    } else if (value instanceof Long) {
      size.addAndGet(8);
      return JsonNodeFactory.instance.numberNode((Long) value);
    } else if (value instanceof Float) {
      size.addAndGet(10);
      return JsonNodeFactory.instance.numberNode((Float) value);
    } else if (value instanceof Double) {
      size.addAndGet(12);
      return JsonNodeFactory.instance.numberNode((Double) value);
    } else if (value instanceof String) {
      // Size of string + quotes. Probably a bit of an underestimate since there will also be
      // escaping.
      size.addAndGet(((String) value).length() + 2);
      return JsonNodeFactory.instance.textNode((String) value);
    } else if (value instanceof byte[]) {
      // Size of base64 string + quotes
      size.addAndGet((((byte[]) value).length * 4 / 3) + 2);
      return JsonNodeFactory.instance.textNode(EntityUtils.encodeBase64Binary((byte[]) value));
    } else if (value instanceof IndexedRecord) {
      IndexedRecord src = (IndexedRecord) value;
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      size.addAndGet(2); // Object braces
      for (Schema.Field field : src.getSchema().getFields()) {
        size.addAndGet(field.name().length() + 3); // name, quotes, : separator
        result.set(field.name(), toJson(src.get(field.pos()), size));
      }
      return result;
    } else if (value instanceof GenericArray) {
      GenericArray src = (GenericArray) value;
      ArrayNode result = JsonNodeFactory.instance.arrayNode();
      size.addAndGet(2); // Array braces
      for (int i = 0; i < src.size(); i++) {
        result.add(toJson(src.get(i), size));
      }
      return result;
    } else if (value instanceof GenericEnumSymbol) {
      String enumString = ((GenericEnumSymbol) value).toString();
      size.addAndGet(enumString.length() + 2);
      return JsonNodeFactory.instance.textNode(enumString);
    } else if (value instanceof Map<?, ?>) {
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      size.addAndGet(2); // Object braces
      for (Map.Entry entry : ((Map<?, ?>) value).entrySet()) {
        Object keyObj = entry.getKey();
        if (!(keyObj instanceof String)) {
          throw new ConversionException("Map keys must be strings");
        }
        String key = (String) keyObj;
        size.addAndGet(key.length() + 3); // name, quotes, : separator
        result.set(key, toJson(entry.getValue(), size));
      }
      return result;
    } else if (value instanceof Byte) { // These are last since they're unsupported, should be rare
      throw new ConversionException("Java Byte is not supported by Avro");
    } else if (value instanceof Character) {
      throw new ConversionException("Java Char is not supported by Avro");
    } else if (value instanceof Short) {
      throw new ConversionException("Java Short is not supported by Avro");
    }

    throw new ConversionException("Unexpected type when converting Avro to JSON: "
                                  + value.getClass().getName());
  }
}
