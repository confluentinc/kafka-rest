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

import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.KafkaRestConfig;
import java.util.Properties;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerInstanceConfig {

  ConsumerInstanceConfig() {
  }

  @Nullable
  public abstract String getId();

  @Nullable
  public abstract String getName();

  public abstract EmbeddedFormat getFormat();

  @Nullable
  public abstract String getAutoOffsetReset();

  @Nullable
  public abstract String getAutoCommitEnable();

  @Nullable
  public abstract Integer getResponseMinBytes();

  @Nullable
  public abstract Integer getRequestWaitMs();

  public final Properties toProperties() {
    Properties properties = new Properties();
    if (getResponseMinBytes() != null) {
      properties.setProperty(
          KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, getResponseMinBytes().toString());
    }
    if (getRequestWaitMs() != null) {
      properties.setProperty(
          KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, getRequestWaitMs().toString());
    }
    return properties;
  }

  public static ConsumerInstanceConfig create(EmbeddedFormat format) {
    return create(
        /* id= */ null,
        /* name= */ null,
        format,
        /* autoOffsetReset= */ null,
        /* autoCommitEnable= */ null,
        /* responseMinBytes= */ null,
        /* requestWaitMs= */ null);
  }

  public static ConsumerInstanceConfig create(
      @Nullable String id,
      @Nullable String name,
      EmbeddedFormat format,
      @Nullable String autoOffsetReset,
      @Nullable String autoCommitEnable,
      @Nullable Integer responseMinBytes,
      @Nullable Integer requestWaitMs
  ) {
    return new AutoValue_ConsumerInstanceConfig(
        id, name, format, autoOffsetReset, autoCommitEnable, responseMinBytes, requestWaitMs);
  }
}
