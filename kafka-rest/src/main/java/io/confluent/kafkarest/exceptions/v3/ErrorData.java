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

package io.confluent.kafkarest.exceptions.v3;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class ErrorData {

  private final String status;

  @Nullable
  private final String code;

  private final String title;

  private final String detail;

  public ErrorData(String status, @Nullable String code, String title, String detail) {
    this.status = requireNonNull(status);
    this.code = code;
    this.title = requireNonNull(title);
    this.detail = requireNonNull(detail);
  }

  @JsonProperty("status")
  public String getStatus() {
    return status;
  }

  @JsonInclude(Include.NON_NULL)
  @JsonProperty("code")
  @Nullable
  public String getCode() {
    return code;
  }

  @JsonProperty("title")
  public String getTitle() {
    return title;
  }

  @JsonProperty("detail")
  public String getDetail() {
    return detail;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorData errorData = (ErrorData) o;
    return status.equals(errorData.status)
        && Objects.equals(code, errorData.code)
        && title.equals(errorData.title)
        && detail.equals(errorData.detail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, code, title, detail);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ErrorData.class.getSimpleName() + "[", "]")
        .add("status='" + status + "'")
        .add("code='" + code + "'")
        .add("title='" + title + "'")
        .add("detail='" + detail + "'")
        .toString();
  }
}
