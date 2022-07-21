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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.kafka.common.config.ConfigException;

public final class CreateConsumerInstanceRequest {

  private static final EmbeddedFormat DEFAULT_FORMAT = EmbeddedFormat.BINARY;

  public static final CreateConsumerInstanceRequest PROTOTYPE =
      new CreateConsumerInstanceRequest(
          /* id= */ null,
          /* name= */ null,
          /* format= */ null,
          /* autoOffsetReset= */ null,
          /* autoCommitEnable= */ null,
          /* responseMinBytes= */ null,
          /* requestWaitMs= */ null);

  @Nullable
  private final String id;

  @Nullable
  private final String name;

  @NotNull
  private final EmbeddedFormat format;

  @Nullable
  private final String autoOffsetReset;

  @Nullable
  private final String autoCommitEnable;

  @Nullable
  private final Integer responseMinBytes;

  @Nullable
  private final Integer requestWaitMs;

  @JsonCreator
  public CreateConsumerInstanceRequest(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("name") @Nullable String name,
      @JsonProperty("format") @Nullable String format,
      @JsonProperty("auto.offset.reset") @Nullable String autoOffsetReset,
      @JsonProperty("auto.commit.enable") @Nullable String autoCommitEnable,
      @JsonProperty("fetch.min.bytes") @JsonAlias("responseMinBytes") @Nullable Integer
          responseMinBytes,
      @JsonProperty("consumer.request.timeout.ms") @JsonAlias("requestWaitMs") @Nullable Integer
          requestWaitMs
  ) {
    this.id = id;
    this.name = name;
    this.format = computeFormat(format);
    this.autoOffsetReset = autoOffsetReset;
    this.autoCommitEnable = autoCommitEnable;
    this.responseMinBytes = computeResponseMinBytes(responseMinBytes);
    this.requestWaitMs = requestWaitMs;
  }

  private static EmbeddedFormat computeFormat(@Nullable String format) {
    if (format == null) {
      return DEFAULT_FORMAT;
    }
    String formatCanonical = format.toUpperCase();
    for (EmbeddedFormat f : EmbeddedFormat.values()) {
      if (f.name().equals(formatCanonical)) {
        return f;
      }
    }
    throw new RestConstraintViolationException(
        "Invalid format type.", RestConstraintViolationException.DEFAULT_ERROR_CODE);
  }

  @Nullable
  private static Integer computeResponseMinBytes(@Nullable Integer responseMinBytes) {
    if (responseMinBytes == null) {
      return null;
    }
    try {
      KafkaRestConfig.PROXY_FETCH_MIN_BYTES_VALIDATOR.ensureValid(
          KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, responseMinBytes);
      return responseMinBytes;
    } catch (ConfigException e) {
      throw Errors.invalidConsumerConfigConstraintException(e);
    }
  }

  @JsonProperty
  @Nullable
  public String getId() {
    return id;
  }

  @JsonProperty
  @Nullable
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getFormat() {
    return format.name().toLowerCase();
  }

  @JsonProperty("auto.offset.reset")
  @Nullable
  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  @JsonProperty("auto.commit.enable")
  @Nullable
  public String getAutoCommitEnable() {
    return autoCommitEnable;
  }

  @JsonProperty("fetch.min.bytes")
  @Nullable
  public Integer getResponseMinBytes() {
    return responseMinBytes;
  }

  @JsonProperty("consumer.request.timeout.ms")
  @Nullable
  public Integer getRequestWaitMs() {
    return requestWaitMs;
  }

  public ConsumerInstanceConfig toConsumerInstanceConfig() {
    return ConsumerInstanceConfig.create(
        id,
        name,
        format,
        autoOffsetReset,
        autoCommitEnable,
        responseMinBytes,
        requestWaitMs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateConsumerInstanceRequest that = (CreateConsumerInstanceRequest) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && format == that.format
        && Objects.equals(autoOffsetReset, that.autoOffsetReset)
        && Objects.equals(autoCommitEnable, that.autoCommitEnable)
        && Objects.equals(responseMinBytes, that.responseMinBytes)
        && Objects.equals(requestWaitMs, that.requestWaitMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, name, format, autoOffsetReset, autoCommitEnable, responseMinBytes, requestWaitMs);
  }

  @Override
  public String toString() {
    return new StringJoiner(
        ", ", CreateConsumerInstanceRequest.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("name='" + name + "'")
        .add("format=" + format)
        .add("autoOffsetReset='" + autoOffsetReset + "'")
        .add("autoCommitEnable='" + autoCommitEnable + "'")
        .add("responseMinBytes=" + responseMinBytes)
        .add("requestWaitMs=" + requestWaitMs)
        .toString();
  }
}
