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

package io.confluent.kafkarest.resources;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.resources.v2.V2ResourcesFeature;
import io.confluent.kafkarest.resources.v3.V3ResourcesFeature;
import jakarta.ws.rs.core.Feature;
import jakarta.ws.rs.core.FeatureContext;

public final class ResourcesFeature implements Feature {
  private final KafkaRestConfig config;

  public ResourcesFeature(KafkaRestConfig config) {
    this.config = requireNonNull(config);
  }

  @Override
  public boolean configure(FeatureContext configurable) {
    if (config.isV2ApiEnabled()) {
      configurable.register(V2ResourcesFeature.class);
    }
    if (config.isV3ApiEnabled()) {
      configurable.register(V3ResourcesFeature.class);
    }
    return true;
  }
}
