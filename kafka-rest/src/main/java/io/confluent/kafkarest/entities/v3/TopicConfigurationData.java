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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A (Topic) configuration resource type.
 */
public final class TopicConfigurationData {

  public static final String ELEMENT_TYPE = "configuration";

  private final String id;

  private final ResourceLink links;

  private final Attributes attributes;

  public TopicConfigurationData(
      String id,
      ResourceLink links,
      String clusterId,
      String topicName,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive) {
    this.id = Objects.requireNonNull(id);
    this.links = Objects.requireNonNull(links);
    attributes =
        new Attributes(clusterId, topicName, name, value, isDefault, isReadOnly, isSensitive);
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaTopicConfiguration";
  }

  @JsonProperty("id")
  public String getId() {
    return id;
  }

  @JsonProperty("links")
  public ResourceLink getLinks() {
    return links;
  }

  @JsonProperty("attributes")
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
    TopicConfigurationData that = (TopicConfigurationData) o;
    return Objects.equals(id, that.id)
        && Objects.equals(links, that.links)
        && Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, links, attributes);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TopicConfigurationData.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("links=" + links)
        .add("attributes=" + attributes)
        .toString();
  }

  public static final class Attributes {

    private final String clusterId;

    private final String topicName;

    private final String name;

    @Nullable
    private final String value;

    private final boolean isDefault;

    private final boolean isReadOnly;

    private final boolean isSensitive;

    public Attributes(
        String clusterId,
        String topicName,
        String name,
        @Nullable String value,
        boolean isDefault,
        boolean isReadOnly,
        boolean isSensitive) {
      this.clusterId = Objects.requireNonNull(clusterId);
      this.topicName = Objects.requireNonNull(topicName);
      this.name = Objects.requireNonNull(name);
      this.value = value;
      this.isDefault = isDefault;
      this.isReadOnly = isReadOnly;
      this.isSensitive = isSensitive;
    }

    @JsonProperty("cluster_id")
    public String getClusterId() {
      return clusterId;
    }

    @JsonProperty("topic_name")
    public String getTopicName() {
      return topicName;
    }

    @JsonProperty("name")
    public String getName() {
      return name;
    }

    @JsonProperty("value")
    @Nullable
    public String getValue() {
      return value;
    }

    @JsonProperty("is_default")
    public boolean isDefault() {
      return isDefault;
    }

    @JsonProperty("is_read_only")
    public boolean isReadOnly() {
      return isReadOnly;
    }

    @JsonProperty("is_sensitive")
    public boolean isSensitive() {
      return isSensitive;
    }

    // CHECKSTYLE:OFF:CyclomaticComplexity
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Attributes that = (Attributes) o;
      return isDefault == that.isDefault
          && isReadOnly == that.isReadOnly
          && isSensitive == that.isSensitive
          && Objects.equals(clusterId, that.clusterId)
          && Objects.equals(topicName, that.topicName)
          && Objects.equals(name, that.name)
          && Objects.equals(value, that.value);
    }
    // CHECKSTYLE:ON:CyclomaticComplexity

    @Override
    public int hashCode() {
      return Objects.hash(clusterId, topicName, name, value, isDefault, isReadOnly, isSensitive);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .add("topicName='" + topicName + "'")
          .add("name='" + name + "'")
          .add("value='" + value + "'")
          .add("isDefault=" + isDefault)
          .add("isReadOnly=" + isReadOnly)
          .add("isSensitive=" + isSensitive)
          .toString();
    }
  }
}
