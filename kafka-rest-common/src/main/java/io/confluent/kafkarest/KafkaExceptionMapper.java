/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.rest.RestConfig;
import io.confluent.rest.entities.ErrorMessage;
import io.confluent.rest.exceptions.GenericExceptionMapper;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaExceptionMapper extends GenericExceptionMapper {

  private static final Logger log = LoggerFactory.getLogger(KafkaExceptionMapper.class);

  //Don't change the error codes, These are fixed for kafka-rest
  public static final int KAFKA_BAD_REQUEST_ERROR_CODE = 40002;
  public static final int KAFKA_AUTHENTICATION_ERROR_CODE = 40101;
  public static final int KAFKA_AUTHORIZATION_ERROR_CODE = 40301;
  public static final int TOPIC_NOT_FOUND_ERROR_CODE = 40401;
  public static final int PARTITION_NOT_FOUND_ERROR_CODE = 40402;
  public static final int KAFKA_UNKNOWN_TOPIC_PARTITION_CODE = 40403;
  public static final int KAFKA_ERROR_ERROR_CODE = 50002;
  public static final int KAFKA_RETRIABLE_ERROR_ERROR_CODE = 50003;
  public static final int BROKER_NOT_AVAILABLE_ERROR_CODE = 50302;

  public KafkaExceptionMapper(RestConfig restConfig) {
    super(restConfig);
  }

  private static final Map<Class<? extends ApiException>, ResponsePair> HANDLED = errorMap();

  private static Map<Class<? extends ApiException>, ResponsePair> errorMap() {
    Map<Class<? extends ApiException>, ResponsePair> errorMap = new HashMap<>();

    errorMap.put(BrokerNotAvailableException.class, new ResponsePair(Status.SERVICE_UNAVAILABLE,
        BROKER_NOT_AVAILABLE_ERROR_CODE));
    errorMap.put(InvalidReplicationFactorException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    // thrown when ACLs are not enabled
    errorMap.put(SecurityDisabledException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(UnsupportedVersionException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(InvalidPartitionsException.class,new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(InvalidRequestException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(UnknownServerException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(UnknownTopicOrPartitionException.class, new ResponsePair(Status.NOT_FOUND,
        KAFKA_UNKNOWN_TOPIC_PARTITION_CODE));
    errorMap.put(PolicyViolationException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(TopicExistsException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    errorMap.put(InvalidConfigurationException.class, new ResponsePair(Status.BAD_REQUEST,
        KAFKA_BAD_REQUEST_ERROR_CODE));
    return errorMap;
  }

  @Override
  public Response toResponse(Throwable exception) {
    if (exception instanceof ExecutionException) {
      return handleException(exception.getCause());
    } else {
      return handleException(exception);
    }
  }

  private Response handleException(final Throwable exception) {
    if (exception instanceof AuthenticationException) {
      return getResponse(exception, Status.UNAUTHORIZED,
          KAFKA_AUTHENTICATION_ERROR_CODE);
    } else if (exception instanceof AuthorizationException) {
      return getResponse(exception, Status.FORBIDDEN,
          KAFKA_AUTHORIZATION_ERROR_CODE);
    } else if (HANDLED.containsKey(exception.getClass())) {
      return getResponse(exception);
    } else if (exception instanceof RetriableException) {
      return getResponse(exception, Status.INTERNAL_SERVER_ERROR,
          KAFKA_RETRIABLE_ERROR_ERROR_CODE);
    } else if (exception instanceof KafkaException) {
      return getResponse(exception, Status.INTERNAL_SERVER_ERROR,
          KAFKA_ERROR_ERROR_CODE);
    } else {
      log.error("Unhandled exception", exception);
      return super.toResponse(exception);
    }
  }

  private Response getResponse(final Throwable exception, final Status status,
                               final int errorCode) {
    ErrorMessage errorMessage = new ErrorMessage(errorCode, exception.getMessage());
    return Response.status(status)
        .entity(errorMessage).build();
  }

  private Response getResponse(final Throwable cause) {
    ResponsePair responsePair = HANDLED.get(cause.getClass());

    ErrorMessage errorMessage = new ErrorMessage(responsePair.errorCode, cause.getMessage());
    return Response.status(responsePair.status)
        .entity(errorMessage).build();
  }


  private static class ResponsePair {
    private final Status status;
    private final int errorCode;

    ResponsePair(Status status, int errorCode) {
      this.status = status;
      this.errorCode = errorCode;
    }
  }
}
