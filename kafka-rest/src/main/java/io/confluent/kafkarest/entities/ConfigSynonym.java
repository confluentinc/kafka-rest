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
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.ConfigEntry;

/**
 * Synonym of a {@link AbstractConfig config}.
 *
 * @see org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym
 * @see <a href=
 *      https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration">
 *      KIP-226 - Dynamic Broker Configuration</a>
 */
@AutoValue
public abstract class ConfigSynonym {

  ConfigSynonym() {
  }

  public abstract String getName();

  @Nullable
  public abstract String getValue();

  public abstract ConfigSource getSource();

  public static ConfigSynonym fromAdminConfigSynonym(ConfigEntry.ConfigSynonym synonym) {
    return new AutoValue_ConfigSynonym(
        synonym.name(), synonym.value(), ConfigSource.fromAdminConfigSource(synonym.source()));
  }
}
