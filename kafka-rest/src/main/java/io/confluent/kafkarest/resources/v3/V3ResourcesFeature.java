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

package io.confluent.kafkarest.resources.v3;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

public final class V3ResourcesFeature implements Feature {

  @Override
  public boolean configure(FeatureContext configurable) {
    configurable.register(AclsResource.class);
    configurable.register(AlterBrokerConfigBatchAction.class);
    configurable.register(AlterClusterConfigBatchAction.class);
    configurable.register(AlterTopicConfigBatchAction.class);
    configurable.register(BrokerConfigsResource.class);
    configurable.register(BrokersResource.class);
    configurable.register(ClusterConfigsResource.class);
    configurable.register(ClustersResource.class);
    configurable.register(ConsumerAssignmentsResource.class);
    configurable.register(ConsumerGroupsResource.class);
    configurable.register(ConsumersResource.class);
    configurable.register(GetReassignmentAction.class);
    configurable.register(ListAllReassignmentsAction.class);
    configurable.register(SearchReassignmentsByTopicAction.class);
    configurable.register(PartitionsResource.class);
    configurable.register(ReplicasResource.class);
    configurable.register(SearchReplicasByBrokerAction.class);
    configurable.register(TopicConfigsResource.class);
    configurable.register(TopicsResource.class);
    return true;
  }
}
