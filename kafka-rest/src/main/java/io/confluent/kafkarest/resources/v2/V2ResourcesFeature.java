/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.resources.v2;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

public final class V2ResourcesFeature implements Feature {

  @Override
  public boolean configure(FeatureContext configurable) {
    configurable.register(BrokersResource.class);
    configurable.register(ConsumersResource.class);
    configurable.register(PartitionsResource.class);
    configurable.register(ProduceToPartitionAction.class);
    configurable.register(ProduceToTopicAction.class);
    configurable.register(RootResource.class);
    configurable.register(TopicsResource.class);
    return true;
  }
}
