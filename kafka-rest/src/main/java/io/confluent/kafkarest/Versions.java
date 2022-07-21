/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest;

import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.MediaType;

public class Versions {

  // Constants for version 2
  public static final String KAFKA_V2_JSON = "application/vnd.kafka.v2+json";
  // This is set < 1 because it is only the most-specific type if there isn't an embedded data type.
  public static final String KAFKA_V2_JSON_WEIGHTED = KAFKA_V2_JSON + "; qs=0.9";
  public static final String KAFKA_V2_JSON_BINARY = "application/vnd.kafka.binary.v2+json";
  public static final String KAFKA_V2_JSON_BINARY_WEIGHTED = KAFKA_V2_JSON_BINARY;
  // "LOW" weightings are used to permit using these for resources like consumer where it might
  // be convenient to always use the same type, but where their use should really be discouraged
  public static final String KAFKA_V2_JSON_BINARY_WEIGHTED_LOW = KAFKA_V2_JSON_BINARY + "; qs=0.1";
  public static final String KAFKA_V2_JSON_AVRO = "application/vnd.kafka.avro.v2+json";
  public static final String KAFKA_V2_JSON_AVRO_WEIGHTED_LOW = KAFKA_V2_JSON_AVRO + "; qs=0.1";

  public static final String KAFKA_V2_JSON_JSON = "application/vnd.kafka.json.v2+json";
  public static final String KAFKA_V2_JSON_JSON_WEIGHTED_LOW = KAFKA_V2_JSON_JSON + "; qs=0.1";

  public static final String KAFKA_V2_JSON_JSON_SCHEMA = "application/vnd.kafka.jsonschema.v2+json";
  public static final String KAFKA_V2_JSON_JSON_SCHEMA_WEIGHTED_LOW =
      KAFKA_V2_JSON_JSON_SCHEMA + "; qs=0.1";

  public static final String KAFKA_V2_JSON_PROTOBUF = "application/vnd.kafka.protobuf.v2+json";
  public static final String KAFKA_V2_JSON_PROTOBUF_WEIGHTED_LOW =
      KAFKA_V2_JSON_PROTOBUF + "; qs=0.1";

  // These are defaults that track the most recent API version. These should always be specified
  // anywhere the latest version is produced/consumed.
  public static final String JSON = "application/json";

  // This is a fallback for when no type is provided. You usually should not need this. It is
  // mostly useful if you have two resource methods for the same endpoints but different Accept
  // or Content-Types. Adding this to one of them makes it the default even if no content type
  // information was specified in the request headers.
  public static final String ANYTHING = "*/*";


  public static final List<String> PREFERRED_RESPONSE_TYPES =
      Arrays.asList(MediaType.APPLICATION_JSON, Versions.KAFKA_V2_JSON);

  // This type is completely generic and carries no actual information about the type of data,
  // but it is the default for request entities if no content type is specified. Well behaving
  // users of the API will always specify the content type, but ad hoc use may omit it. We treat
  // this as JSON since that's all we currently support.
  public static final String GENERIC_REQUEST = "application/octet-stream";
}
