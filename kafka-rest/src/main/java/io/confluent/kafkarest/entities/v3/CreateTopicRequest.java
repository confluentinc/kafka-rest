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

package io.confluent.kafkarest.entities.v3;

import static java.util.Collections.emptyMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Request body for {@code POST /v3/clusters/<clusterId>/topics} requests.
 */
public final class CreateTopicRequest {

  @NotNull
  @Nullable
  private final Data data;

  @JsonCreator
  public CreateTopicRequest(@JsonProperty("data") @Nullable Data data) {
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
    CreateTopicRequest that = (CreateTopicRequest) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CreateTopicRequest.class.getSimpleName() + "[", "]")
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

      @NotNull
      @Nullable
      private final String topicName;

      private final int partitionsCount;

      private final short replicationFactor;

      @Nullable
      private final List<Config> configs;

      @JsonCreator
      public Attributes(
          @JsonProperty("topic_name") @Nullable String topicName,
          @JsonProperty("partitions_count") int partitionsCount,
          @JsonProperty("replication_factor") short replicationFactor,
          @JsonProperty("configs") @Nullable List<Config> configs
      ) {
        this.topicName = topicName;
        this.partitionsCount = partitionsCount;
        this.replicationFactor = replicationFactor;
        this.configs = configs;
      }

      @Nullable
      public String getTopicName() {
        return topicName;
      }

      public int getPartitionsCount() {
        return partitionsCount;
      }

      public short getReplicationFactor() {
        return replicationFactor;
      }

      public Map<String, String> getConfigs() {
        if (this.configs == null) {
          return emptyMap();
        }

        HashMap<String, String> configs = new HashMap<>();
        for (Config config : this.configs) {
          configs.put(config.getName(), config.getValue());
        }
        return configs;
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
        return partitionsCount == that.partitionsCount
            && replicationFactor == that.replicationFactor
            && Objects.equals(topicName, that.topicName)
            && Objects.equals(configs, that.configs);
      }

      @Override
      public int hashCode() {
        return Objects.hash(topicName, partitionsCount, replicationFactor, configs);
      }

      @Override
      public String toString() {
        return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
            .add("topicName='" + topicName + "'")
            .add("partitionsCount=" + partitionsCount)
            .add("replicationFactor=" + replicationFactor)
            .add("configs=" + configs)
            .toString();
      }

      public static final class Config {

        @NotNull
        @Nullable
        private final String name;

        @Nullable
        private final String value;

        @JsonCreator
        public Config(
            @JsonProperty("name") @Nullable String name,
            @JsonProperty("value") @Nullable String value) {
          this.name = name;
          this.value = value;
        }

        @Nullable
        public String getName() {
          return name;
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
          Config that = (Config) o;
          return Objects.equals(name, that.name) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
          return Objects.hash(name, value);
        }

        @Override
        public String toString() {
          return new StringJoiner(", ", Config.class.getSimpleName() + "[", "]")
              .add("name='" + name + "'")
              .add("value='" + value + "'")
              .toString();
        }
      }
    }
  }
}
