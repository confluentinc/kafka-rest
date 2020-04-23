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

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response.Status;

/**
 * An exception that can be returned as an HTTP response.
 */
public class StatusCodeException extends RuntimeException {

  private final Status status;

  @Nullable
  private final Integer code;

  private final String title;

  private final String detail;

  public static StatusCodeException create(Status status, String title, String detail) {
    return new StatusCodeException(status, title, detail);
  }

  StatusCodeException(Status status, String title, String detail) {
    this(status, /* code= */ null, title, detail);
  }

  public static StatusCodeException create(
      Status status, String title, String detail, Throwable cause) {
    return new StatusCodeException(status, title, detail, cause);
  }

  StatusCodeException(Status status, String title, String detail, Throwable cause) {
    this(status, /* code= */ null, title, detail, cause);
  }

  StatusCodeException(Status status, @Nullable Integer code, String title, String detail) {
    super(detail);
    this.status = requireNonNull(status);
    this.code = code;
    this.title = requireNonNull(title);
    this.detail = requireNonNull(detail);
  }

  StatusCodeException(
      Status status, @Nullable Integer code, String title, String detail, Throwable cause) {
    super(detail, cause);
    this.status = requireNonNull(status);
    this.code = code;
    this.title = requireNonNull(title);
    this.detail = requireNonNull(detail);
  }

  public Status getStatus() {
    return status;
  }

  public Optional<Integer> getCode() {
    return Optional.ofNullable(code);
  }

  public String getTitle() {
    return title;
  }

  public String getDetail() {
    return detail;
  }
}
