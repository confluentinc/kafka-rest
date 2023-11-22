/*
 * Copyright 2021 Confluent Inc.
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

public class BadRequestException extends StatusCodeException {

  public BadRequestException(String detail) {
    this("Bad Request", detail);
  }

  public BadRequestException(String detail, Throwable cause) {
    this("Bad Request", detail, cause);
  }

  public BadRequestException(String title, String detail) {
    super(Status.BAD_REQUEST, title, detail);
  }

  public BadRequestException(String title, String detail, Throwable cause) {
    super(Status.BAD_REQUEST, title, detail, cause);
  }
}
