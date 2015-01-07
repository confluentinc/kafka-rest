package io.confluent.kafkarest;

import io.confluent.rest.exceptions.NotFoundException;
import io.confluent.rest.exceptions.RestException;

public class Errors {
  public final static String TOPIC_NOT_FOUND_MESSAGE = "Topic not found.";
  public final static int TOPIC_NOT_FOUND_ERROR_CODE = 40401;
  public static RestException topicNotFoundException() {
    return new NotFoundException(TOPIC_NOT_FOUND_MESSAGE, TOPIC_NOT_FOUND_ERROR_CODE);
  }

  public final static String PARTITION_NOT_FOUND_MESSAGE = "Partition not found.";
  public final static int PARTITION_NOT_FOUND_ERROR_CODE = 40402;
  public static RestException partitionNotFoundException() {
    return new NotFoundException(PARTITION_NOT_FOUND_MESSAGE, PARTITION_NOT_FOUND_ERROR_CODE);
  }

  public final static String CONSUMER_INSTANCE_NOT_FOUND_MESSAGE = "Consumer instance not found.";
  public final static int CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE = 40403;
  public static RestException consumerInstanceNotFoundException() {
    return new NotFoundException(CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
                                 CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE);
  }
}
