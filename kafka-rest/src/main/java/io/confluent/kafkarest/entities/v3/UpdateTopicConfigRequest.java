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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Request body for {@code PUT /v3/clusters/<clusterId>/topics/<topicName>/configs/<name>}
 * requests.
 */
public final class UpdateTopicConfigRequest {

  @NotNull
  @Nullable
  private final Data data;

  @JsonCreator
  public UpdateTopicConfigRequest(@JsonProperty("data") @Nullable Data data) {
    this.data = data;
  }

  @Nullable
  public Data getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateTopicConfigRequest that = (UpdateTopicConfigRequest) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ",
        UpdateTopicConfigRequest.class.getSimpleName() + "[", "]")
        .add("data=" + data)
        .toString();
  }

  public static final class Data {

    @NotNull
    @Nullable
    private final Attributes attributes;

    @JsonCreator
    public Data(@JsonProperty("attributes") @Nullable Attributes attributes) {
      this.attributes = attributes;
    }

    @Nullable
    public Attributes getAttributes() {
      return attributes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Data data = (Data) o;
      return Objects.equals(attributes, data.attributes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(attributes);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Data.class.getSimpleName() + "[", "]")
          .add("attributes=" + attributes)
          .toString();
    }

    public static final class Attributes {

      @Nullable
      private final String value;

      @JsonCreator
      public Attributes(@JsonProperty("value") @Nullable String value) {
        this.value = value;
      }

      @Nullable
      public String getValue() {
        return value;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        Attributes that = (Attributes) o;
        return Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
        return Objects.hash(value);
      }

      @Override
      public String toString() {
        return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
            .add("value='" + value + "'")
            .toString();
      }
    }
  }
}
