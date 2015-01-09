package io.confluent.kafkarest;

import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestException;

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
}
