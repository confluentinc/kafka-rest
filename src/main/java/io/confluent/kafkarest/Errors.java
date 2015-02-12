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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;

import javax.ws.rs.core.Response;

import io.confluent.rest.exceptions.RestConstraintViolationException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;
import kafka.common.InvalidConfigException;

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


  public final static String CONSUMER_FORMAT_MISMATCH_MESSAGE =
      "The requested embedded data format does not match the deserializer for this consumer "
      + "instance";
  public final static int CONSUMER_FORMAT_MISMATCH_ERROR_CODE = 40601;

  public static RestException consumerFormatMismatch() {
    return new RestException(CONSUMER_FORMAT_MISMATCH_MESSAGE,
                             Response.Status.NOT_ACCEPTABLE.getStatusCode(),
                             CONSUMER_FORMAT_MISMATCH_ERROR_CODE);
  }


  public final static String CONSUMER_ALREADY_SUBSCRIBED_MESSAGE =
      "Consumer cannot subscribe the the specified target because it has already subscribed to "
      + "other topics.";
  public final static int CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE = 40901;

  public static RestException consumerAlreadySubscribedException() {
    return new RestException(CONSUMER_ALREADY_SUBSCRIBED_MESSAGE,
                             Response.Status.CONFLICT.getStatusCode(),
                             CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE);
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

  public final static String INVALID_CONSUMER_CONFIG_MESSAGE = "Invalid consumer configuration: ";
  public final static int INVALID_CONSUMER_CONFIG_ERROR_CODE = 42204;

  public static RestConstraintViolationException invalidConsumerConfigException(
      InvalidConfigException e) {
    return new RestConstraintViolationException(INVALID_CONSUMER_CONFIG_MESSAGE + e.getMessage(),
                                                INVALID_CONSUMER_CONFIG_ERROR_CODE);
  }

  public final static int KAFKA_EXCEPTION_CODE = 2;
  public final static int KAFKA_RETRIABLE_EXCEPTION_CODE = 3;

  public final static String UNEXPECTED_PRODUCER_EXCEPTION
      = "Unexpected non-Kafka exception returned by Kafka";

  public static int codeFromProducerException(Throwable e) {
    if (e instanceof RetriableException) {
      return KAFKA_RETRIABLE_EXCEPTION_CODE;
    } else if (e instanceof KafkaException) {
      return KAFKA_EXCEPTION_CODE;
    } else {
      // We shouldn't see any non-Kafka exceptions, but this covers us in case we do see an
      // unexpected error. In that case we fail the entire request -- this loses information
      // since some messages may have been produced correctly, but is the right thing to do from
      // a REST perspective since there was an internal error with the service while processing
      // the request.
      throw new RestServerErrorException(UNEXPECTED_PRODUCER_EXCEPTION,
                                         RestServerErrorException.DEFAULT_ERROR_CODE, e
      );
    }
  }


  // This is a catch-all for consumer exceptions since we still use the old consumer and it's
  // exceptions aren't easily classifiable (and we don't want to try to enumerate every single
  // one with a more specific error code).
  public static RestServerErrorException consumerGeneralOperationException(Exception e) {
    return new RestServerErrorException("Consumer operation failed: " + e.getMessage(),
                                        RestServerErrorException.DEFAULT_ERROR_CODE, e);
  }
}
