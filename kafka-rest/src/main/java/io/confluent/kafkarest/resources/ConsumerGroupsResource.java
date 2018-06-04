/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerEntity;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.TopicName;
import io.confluent.rest.annotations.PerformanceMetric;
import scala.Option;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.util.List;
import java.util.Set;

/**
 * Provides metadata about consumers groups
 */
@Path("/groups")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
        Versions.JSON_WEIGHTED, Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
        Versions.GENERIC_REQUEST, Versions.KAFKA_V2_JSON})
public class ConsumerGroupsResource {

  private final KafkaRestContext context;

  /**
   * <p>Create consumers group resource</p>
   *
   * @param context - context of rest application
   */
  public ConsumerGroupsResource(KafkaRestContext context) {
    this.context = context;
  }

  /**
   * <p>Get consumer group list</p>
   * <p>Example: http://127.0.0.1:2081/groups/</p>
   *
   * @return List[ConsumerGroup] -
   *     [{"groupId":"testGroup", "coordinator": {"host": "127.0.0.1", "port": "123"}}]
   */
  @GET
  @PerformanceMetric("groups.list")
  public List<ConsumerGroup> list(@QueryParam("offset") Integer offset,
                                  @QueryParam("count") Integer count) {
    return context.getGroupMetadataObserver()
            .getConsumerGroupList(Option.apply(offset), Option.apply(count));
  }

  /**
   * <p>Get partitions list for group groupId</p>
   * <p>Example: http://127.0.0.1:2081/groups/testGroup/partitions</p>
   *
   * @return ConsumerEntity
   */
  @GET
  @Path("/{groupId}/partitions")
  @PerformanceMetric("groups.get.partitions")
  public ConsumerEntity getPartitionsInformation(@PathParam("groupId") String groupId) {
    return context.getGroupMetadataObserver().getConsumerGroupInformation(groupId);
  }

  /**
   * <p>Get topics list for group groupId</p>
   * <p>Example: http://127.0.0.1:2081/groups/testGroup/topics</p>
   *
   * @return Set[TopicName]
   */
  @GET
  @Path("/{groupId}/topics")
  @PerformanceMetric("groups.get.topics")
  public Set<TopicName> getTopics(@PathParam("groupId") String groupId,
                                  @QueryParam("offset") Integer offset,
                                  @QueryParam("count") Integer count) {
    return context.getGroupMetadataObserver()
            .getConsumerGroupTopicInformation(groupId, Option.apply(offset), Option.apply(count));
  }

  /**
   * <p>Get partitions list for group groupId</p>
   * <p>Example: http://127.0.0.1:2081/groups/testGroup/topics/testTopic?offset=10&count=10</p>
   *
   * @return ConsumerEntity
   */
  @GET
  @Path("/{groupId}/topics/{topic}")
  @PerformanceMetric("groups.get.topic.partitions")
  public ConsumerEntity getPartitionsInformationByTopic(@PathParam("groupId") String groupId,
                                                        @PathParam("topic") String topic,
                                                        @QueryParam("offset") Integer offset,
                                                        @QueryParam("count") Integer count) {
    return context.getGroupMetadataObserver()
            .getConsumerGroupInformation(groupId,
                    Option.apply(topic),
                    Option.apply(offset),
                    Option.apply(count));
  }
}
