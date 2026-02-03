/*
 * Copyright 2026 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.rest.entities.ErrorMessage;
import java.util.Objects;

/**
 * Extended error message that includes a schema error code for Odyssey schema validation errors.
 * The schema_error_code field is only included in the JSON response when non-null.
 */
public class SchemaErrorMessage extends ErrorMessage {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final Integer schemaErrorCode;

  public SchemaErrorMessage(int errorCode, String message, Integer schemaErrorCode) {
    super(errorCode, message);
    this.schemaErrorCode = schemaErrorCode;
  }

  @JsonProperty("schema_error_code")
  public Integer getSchemaErrorCode() {
    return schemaErrorCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SchemaErrorMessage that = (SchemaErrorMessage) o;
    return Objects.equals(schemaErrorCode, that.schemaErrorCode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schemaErrorCode);
  }
}
