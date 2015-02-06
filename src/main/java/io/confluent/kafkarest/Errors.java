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

package io.confluent.kafkarest;

import io.confluent.rest.exceptions.RestConstraintViolationException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class Errors {

  public final static String TOPIC_NOT_FOUND_MESSAGE = "Topic not found.";
  public final static int TOPIC_NOT_FOUND_ERROR_CODE = 40401;

  public static RestException topicNotFoundException() {
    return new RestNotFoundException(TOPIC_NOT_FOUND_MESSAGE, TOPIC_NOT_FOUND_ERROR_CODE);
  }

  public final static String PARTITION_NOT_FOUND_MESSAGE = "Partition not found.";
  public final static int PARTITION_NOT_FOUND_ERROR_CODE = 40402;

  public static RestException partitionNotFoundException() {
    return new RestNotFoundException(PARTITION_NOT_FOUND_MESSAGE, PARTITION_NOT_FOUND_ERROR_CODE);
  }

  public final static String CONSUMER_INSTANCE_NOT_FOUND_MESSAGE = "Consumer instance not found.";
  public final static int CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE = 40403;

  public static RestException consumerInstanceNotFoundException() {
    return new RestNotFoundException(CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
                                     CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE);
  }

  public final static String KEY_SCHEMA_MISSING_MESSAGE = "Request includes keys but does not "
                                                          + "include key schema";
  public final static int KEY_SCHEMA_MISSING_ERROR_CODE = 42201;

  public static RestConstraintViolationException keySchemaMissingException() {
    return new RestConstraintViolationException(KEY_SCHEMA_MISSING_MESSAGE,
                                                KEY_SCHEMA_MISSING_ERROR_CODE);

  }


  public final static String VALUE_SCHEMA_MISSING_MESSAGE = "Request includes values but does not "
                                                            + "include value schema";
  public final static int VALUE_SCHEMA_MISSING_ERROR_CODE = 42202;

  public static RestConstraintViolationException valueSchemaMissingException() {
    return new RestConstraintViolationException(VALUE_SCHEMA_MISSING_MESSAGE,
                                                VALUE_SCHEMA_MISSING_ERROR_CODE);

  }

  public final static String JSON_AVRO_CONVERSION_MESSAGE = "Conversion of JSON to Avro failed.";
  public final static int JSON_AVRO_CONVERSION_ERROR_CODE = 42203;

  public static RestConstraintViolationException jsonAvroConversionException() {
    return new RestConstraintViolationException(JSON_AVRO_CONVERSION_MESSAGE,
                                                JSON_AVRO_CONVERSION_ERROR_CODE);

  }
}
