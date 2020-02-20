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

import static io.confluent.kafkarest.Errors.KAFKA_ERROR_ERROR_CODE;

import io.confluent.rest.exceptions.RestServerErrorException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;

public class Utils {

  public static final String UNEXPECTED_PRODUCER_EXCEPTION
      = "Unexpected non-Kafka exception returned by Kafka";

  public static int errorCodeFromProducerException(Throwable e) {
    if (e instanceof AuthenticationException) {
      return Errors.KAFKA_AUTHENTICATION_ERROR_CODE;
    } else if (e instanceof AuthorizationException) {
      return Errors.KAFKA_AUTHORIZATION_ERROR_CODE;
    } else if (e instanceof RetriableException) {
      return Errors.KAFKA_RETRIABLE_ERROR_ERROR_CODE;
    } else if (e instanceof KafkaException) {
      return KAFKA_ERROR_ERROR_CODE;
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
}
