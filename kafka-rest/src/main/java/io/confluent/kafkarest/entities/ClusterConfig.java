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
import org.apache.kafka.common.config.ConfigResource;

/**
 * A Kafka cluster-wide dynamic default config.
 */
@AutoValue
public abstract class ClusterConfig extends AbstractConfig {

  ClusterConfig() {
  }

  public abstract Type getType();

  public static ClusterConfig.Builder builder() {
    return new AutoValue_ClusterConfig.Builder();
  }

  public static ClusterConfig create(
      String clusterId,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive,
      ConfigSource source,
      List<ConfigSynonym> synonyms,
      Type type) {
    return builder()
        .setClusterId(clusterId)
        .setName(name)
        .setValue(value)
        .setDefault(isDefault)
        .setReadOnly(isReadOnly)
        .setSensitive(isSensitive)
        .setSource(source)
        .setSynonyms(synonyms)
        .setType(type)
        .build();
  }

  /**
   * A builder for {@link ClusterConfig}.
   */
  @AutoValue.Builder
  public abstract static class Builder extends AbstractConfig.Builder<ClusterConfig, Builder> {

    Builder() {
    }

    public abstract Builder setType(Type type);
  }

  /**
   * The type of the {@link ClusterConfig}.
   */
  public enum Type {

    /**
     * A cluster-wide dynamic default broker config.
     */
    BROKER(ConfigResource.Type.BROKER);

    private final ConfigResource.Type adminType;

    Type(ConfigResource.Type adminType) {
      this.adminType = adminType;
    }

    public ConfigResource.Type getAdminType() {
      return adminType;
    }
  }
}
