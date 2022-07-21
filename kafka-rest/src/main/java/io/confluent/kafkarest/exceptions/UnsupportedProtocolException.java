/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.exceptions;

import javax.ws.rs.core.Response.Status;

/**
 * An exception that indicates a Kafka operation has been attempted for an unsupported protocol
 * version.
 */
public final class UnsupportedProtocolException extends StatusCodeException {

  public UnsupportedProtocolException(String message) {
    super(Status.BAD_GATEWAY, "Kafka protocol version not supported.", message);
  }

  public UnsupportedProtocolException(String message, Throwable cause) {
    super(Status.BAD_GATEWAY, "Kafka protocol version not supported.", message, cause);
  }
}
