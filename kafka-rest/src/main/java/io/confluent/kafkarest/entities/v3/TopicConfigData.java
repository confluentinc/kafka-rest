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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.entities.ConfigSource;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A (Topic) config resource type.
 */
@JsonIgnoreProperties(value = {"type"}, allowGetters = true)
public final class TopicConfigData {

  public static final String ELEMENT_TYPE = "config";

  private final String id;

  private final ResourceLink links;

  private final Attributes attributes;

  // CHECKSTYLE:OFF:ParameterNumber
  public TopicConfigData(
      String id,
      ResourceLink links,
      String clusterId,
      String topicName,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive,
      ConfigSource source,
      List<ConfigSynonymData> synonyms) {
    this(
        id,
        links,
        new Attributes(
            clusterId,
            topicName,
            name,
            value,
            isDefault,
            isReadOnly,
            isSensitive,
            source,
            synonyms));
  }
  // CHECKSTYLE:ON:ParameterNumber

  @JsonCreator
  public TopicConfigData(
          @JsonProperty("id") String id,
          @JsonProperty("links") ResourceLink links,
          @JsonProperty("attributes") Attributes attributes
  ) {
    this.id = requireNonNull(id);
    this.links = requireNonNull(links);
    this.attributes = attributes;
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaTopicConfig";
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
    TopicConfigData that = (TopicConfigData) o;
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
    return new StringJoiner(", ", TopicConfigData.class.getSimpleName() + "[", "]")
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

    private final ConfigSource source;

    private final List<ConfigSynonymData> synonyms;

    @JsonCreator
    public Attributes(
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("topic_name") String topicName,
        @JsonProperty("name") String name,
        @JsonProperty("value") @Nullable String value,
        @JsonProperty("is_default") boolean isDefault,
        @JsonProperty("is_read_only") boolean isReadOnly,
        @JsonProperty("is_sensitive") boolean isSensitive,
        @JsonProperty("source") ConfigSource source,
        @JsonProperty("synonyms") List<ConfigSynonymData> synonyms) {
      this.clusterId = clusterId;
      this.topicName = topicName;
      this.name = name;
      this.value = value;
      this.isDefault = isDefault;
      this.isReadOnly = isReadOnly;
      this.isSensitive = isSensitive;
      this.source = source;
      this.synonyms = synonyms;
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

    @JsonProperty("source")
    public ConfigSource getSource() {
      return source;
    }

    @JsonProperty("synonyms")
    public List<ConfigSynonymData> getSynonyms() {
      return synonyms;
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
      return Objects.equals(clusterId, that.clusterId)
          && Objects.equals(topicName, that.topicName)
          && Objects.equals(name, that.name)
          && Objects.equals(value, that.value)
          && isDefault == that.isDefault
          && isReadOnly == that.isReadOnly
          && isSensitive == that.isSensitive
          && Objects.equals(source, that.source)
          && Objects.equals(synonyms, that.synonyms);
    }
    // CHECKSTYLE:ON:CyclomaticComplexity

    @Override
    public int hashCode() {
      return Objects.hash(
          clusterId, topicName, name, value, isDefault, isReadOnly, isSensitive, source, synonyms);
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
          .add("source=" + source)
          .add("synonyms=" + synonyms)
          .toString();
    }
  }
}
