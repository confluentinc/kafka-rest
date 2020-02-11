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

package io.confluent.kafkarest.resources.v1;

import io.confluent.kafkarest.KafkaRestContext;
import java.util.Objects;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

public final class V1ResourcesFeature implements Feature {
  private final KafkaRestContext context;

  public V1ResourcesFeature(KafkaRestContext context) {
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public boolean configure(FeatureContext configurable) {
    configurable.register(new BrokersResource(context));
    configurable.register(new ConsumersResource(context));
    return true;
  }
}
