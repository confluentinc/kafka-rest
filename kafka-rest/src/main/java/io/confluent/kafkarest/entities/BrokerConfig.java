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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Kafka broker config
 */
@AutoValue
public abstract class BrokerConfig extends AbstractConfig {

  BrokerConfig() {
  }

  public abstract int getBrokerId();

  public static Builder builder() {
    return new AutoValue_BrokerConfig.Builder();
  }

  public static BrokerConfig create(
      String clusterId,
      int brokerId,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive,
      ConfigSource source,
      List<ConfigSynonym> synonyms) {
    return builder()
        .setClusterId(clusterId)
        .setName(name)
        .setValue(value)
        .setDefault(isDefault)
        .setReadOnly(isReadOnly)
        .setSensitive(isSensitive)
        .setSource(source)
        .setSynonyms(synonyms)
        .setBrokerId(brokerId)
        .build();
  }

  /**
   * A builder for {@link BrokerConfig}.
   */
  @AutoValue.Builder
  public abstract static class Builder extends AbstractConfig.Builder<BrokerConfig, Builder> {

    Builder() {
    }

    public abstract Builder setBrokerId(int brokerId);
  }
}
