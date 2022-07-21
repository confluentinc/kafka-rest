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
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.entities.ConfigSource;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class AbstractConfigData extends Resource {

  AbstractConfigData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("name")
  public abstract String getName();

  @JsonProperty("value")
  public abstract Optional<String> getValue();

  @JsonProperty("is_default")
  public abstract boolean isDefault();

  @JsonProperty("is_read_only")
  public abstract boolean isReadOnly();

  @JsonProperty("is_sensitive")
  public abstract boolean isSensitive();

  @JsonProperty("source")
  public abstract ConfigSource getSource();

  @JsonProperty("synonyms")
  public abstract ImmutableList<ConfigSynonymData> getSynonyms();

  public abstract static class Builder<BuilderT extends Builder<BuilderT>>
      extends Resource.Builder<BuilderT> {

    Builder() {
    }

    public abstract BuilderT setClusterId(String clusterId);

    public abstract BuilderT setName(String name);

    public abstract BuilderT setValue(@Nullable String value);

    public abstract BuilderT setDefault(boolean isDefault);

    public abstract BuilderT setReadOnly(boolean isReadOnly);

    public abstract BuilderT setSensitive(boolean isSensitive);

    public abstract BuilderT setSource(ConfigSource source);

    public abstract BuilderT setSynonyms(List<ConfigSynonymData> synonyms);
  }
}
