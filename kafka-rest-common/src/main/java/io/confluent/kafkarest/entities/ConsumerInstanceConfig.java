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

package io.confluent.kafkarest.entities;

import io.confluent.kafkarest.KafkaRestConfig;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nullable;

public final class ConsumerInstanceConfig {

  @Nullable
  private final String id;

  @Nullable
  private final String name;

  private final EmbeddedFormat format;

  @Nullable
  private final String autoOffsetReset;

  @Nullable
  private final String autoCommitEnable;

  @Nullable
  private final Integer responseMinBytes;

  @Nullable
  private final Integer requestWaitMs;

  public ConsumerInstanceConfig(EmbeddedFormat format) {
    this(
        /* id= */ null,
        /* name= */ null,
        format,
        /* autoOffsetReset= */ null,
        /* autoCommitEnable= */ null,
        /* responseMinBytes= */ null,
        /* requestWaitMs= */ null);
  }

  public ConsumerInstanceConfig(
      @Nullable String id,
      @Nullable String name,
      EmbeddedFormat format,
      @Nullable String autoOffsetReset,
      @Nullable String autoCommitEnable,
      @Nullable Integer responseMinBytes,
      @Nullable Integer requestWaitMs
  ) {
    this.id = id;
    this.name = name;
    this.format = Objects.requireNonNull(format);
    this.responseMinBytes = responseMinBytes;
    this.requestWaitMs = requestWaitMs;
    this.autoOffsetReset = autoOffsetReset;
    this.autoCommitEnable = autoCommitEnable;
  }

  @Nullable
  public String getId() {
    return id;
  }

  @Nullable
  public String getName() {
    return name;
  }

  public EmbeddedFormat getFormat() {
    return format;
  }

  @Nullable
  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  @Nullable
  public String getAutoCommitEnable() {
    return autoCommitEnable;
  }

  @Nullable
  public Integer getResponseMinBytes() {
    return this.responseMinBytes;
  }

  @Nullable
  public Integer getRequestWaitMs() {
    return this.requestWaitMs;
  }

  public Properties toProperties() {
    Properties properties = new Properties();
    if (responseMinBytes != null) {
      properties.setProperty(
          KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, responseMinBytes.toString());
    }
    if (requestWaitMs != null) {
      properties.setProperty(
          KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, requestWaitMs.toString());
    }
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsumerInstanceConfig that = (ConsumerInstanceConfig) o;
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
}
