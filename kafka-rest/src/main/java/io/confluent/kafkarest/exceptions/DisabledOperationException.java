package io.confluent.kafkarest.exceptions;

import javax.ws.rs.core.Response.Status;

public final class DisabledOperationException extends StatusCodeException {

  public DisabledOperationException(Status status) {
    super(
        status,
        "Operation not supported.",
        "The operation attempted is not supported by this server.");
  }
}
