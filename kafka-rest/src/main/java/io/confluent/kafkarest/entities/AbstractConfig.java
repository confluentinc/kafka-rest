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

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Kafka config.
 */
public abstract class AbstractConfig {

  AbstractConfig() {
  }

  public abstract String getClusterId();

  public abstract String getName();

  @Nullable
  public abstract String getValue();

  public abstract boolean isDefault();

  public abstract boolean isReadOnly();

  public abstract boolean isSensitive();

  public abstract ConfigSource getSource();

  public abstract ImmutableList<ConfigSynonym> getSynonyms();

  /**
   * A builder for {@link AbstractConfig}.
   */
  public abstract static class Builder<T extends AbstractConfig, B extends Builder<T, B>> {

    public abstract B setClusterId(String clusterId);

    public abstract B setName(String name);

    public abstract B setValue(@Nullable String value);

    public abstract B setDefault(boolean isDefault);

    public abstract B setReadOnly(boolean isReadOnly);

    public abstract B setSensitive(boolean isSensitive);

    public abstract B setSource(ConfigSource source);

    public abstract B setSynonyms(List<ConfigSynonym> synonyms);

    public abstract T build();
  }
}
