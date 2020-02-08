/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.kafkarest.entities.ConsumerGroupSubscription;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
   * @param pageSize - Optional parameter. Used for paging.
   *             Restrict count of returned entities with group information.
   * @param pageOffset - Optional parameter. Used for paging.
   *               Offset which starts return records from.
   * @return List of group names with group coordinator host of each group.
   *     [{"groupId":"testGroup", "coordinator": {"host": "127.0.0.1", "port": "123"}}]
   */
  @GET
  @PerformanceMetric("groups.list")
  public List<ConsumerGroup> list(@QueryParam("page_offset") Integer pageOffset,
                                  @QueryParam("page_size") Integer pageSize)
          throws Exception {
    final boolean needPartOfData = Optional.ofNullable(pageOffset).isPresent()
        && Optional.ofNullable(pageSize).isPresent()
        && pageSize > 0;
    if (needPartOfData) {
      return context.getGroupMetadataObserver()
          .getPagedConsumerGroupList(pageOffset, pageSize);
    }
    return context.getGroupMetadataObserver().getConsumerGroupList();
  }

  /**
   * <p>Get partitions list for group groupId</p>
   * <p>Example: http://127.0.0.1:2081/groups/testGroup/partitions</p>
   *
   * @param groupId - Group name.
   * @return Consumer subscription information. Include group offsets, lags and
   *      group coordinator information.
   *      { "topicPartitionList":[
   *         { "consumerId":"consumer-1-88792db6-99a2-4064-aad2-38be12b32e88",
   *            "consumerIp":"/{some_ip}",
   *            "topicName":"1",
   *            "partitionId":0,
   *            "currentOffset":15338734,
   *            "lag":113812,
   *            "endOffset":15452546},
   *         { "consumerId":"consumer-1-88792db6-99a2-4064-aad2-38be12b32e88",
   *            "consumerIp":"/{some_ip}",
   *            "topicName":"1",
   *            "partitionId":1,
   *            "currentOffset":15753823,
   *            "lag":136160,
   *            "endOffset":15889983},
   *         { "consumerId":"consumer-1-88792db6-99a2-4064-aad2-38be12b32e88",
   *            "consumerIp":"/{some_ip}",
   *            "topicName":"1",
   *            "partitionId":2,
   *            "currentOffset":15649419,
   *            "lag":133052,
   *            "endOffset":15782471}],
   *        "topicPartitionCount":3,
   *        "coordinator":{ "host":"{coordinator_host_name}","port":9496 }
   *      }
   */
  @GET
  @Path("/{groupId}/partitions")
  @PerformanceMetric("groups.get.partitions")
  public ConsumerGroupSubscription getPartitionsInformation(@PathParam("groupId") String groupId)
          throws Exception {
    return context.getGroupMetadataObserver().getConsumerGroupInformation(groupId);
  }

  /**
   * <p>Get topics list for group groupId</p>
   * <p>Example: http://127.0.0.1:2081/groups/testGroup/topics</p>
   *
   * @param groupId - Group name.
   * @param pageSize - Optional parameter. Used for paging.
   *             Restrict count of returned entities with group information.
   * @param pageOffset - Optional parameter. Used for paging.
   *               Offset which starts return records from.
   * @return Topic names who are read by specified consumer group.
   *       [{"name":"1"}]
   */
  @GET
  @Path("/{groupId}/topics")
  @PerformanceMetric("groups.get.topics")
  public Set<Topic> getTopics(@PathParam("groupId") String groupId,
                              @QueryParam("page_offset") Integer pageOffset,
                              @QueryParam("page_size") Integer pageSize) throws Exception {
    final boolean needPartOfData = Optional.ofNullable(pageOffset).isPresent()
        && Optional.ofNullable(pageSize).isPresent()
        && pageSize > 0;
    if (needPartOfData) {
      return context.getGroupMetadataObserver()
          .getPagedConsumerGroupTopicInformation(groupId,
              pageOffset,
              pageSize);
    }
    return context.getGroupMetadataObserver()
        .getConsumerGroupTopicInformation(groupId);
  }

  /**
   * <p>Get partitions list for group groupId</p>
   * <p>Example: http://127.0.0.1:2081/groups/testGroup/topics/testTopic?page_offset=10&page_size=10</p>
   *
   * @param groupId - Group name.
   * @param topic - Topic name.
   * @param pageSize - Optional parameter. Used for paging.
   *             Restrict count of returned entities with group information.
   * @param pageOffset - Optional parameter. Used for paging.
   *             Offset which starts return records from.
   * @return Consumer subscription information. Include group offsets, lags and
   *      group coordinator information.
   *      { "topicPartitionList":[
   *         { "consumerId":"consumer-1-88792db6-99a2-4064-aad2-38be12b32e88",
   *            "consumerIp":"/{some_ip}",
   *            "topicName":"1",
   *            "partitionId":0,
   *            "currentOffset":15338734,
   *            "lag":113812,
   *            "endOffset":15452546},
   *         { "consumerId":"consumer-1-88792db6-99a2-4064-aad2-38be12b32e88",
   *            "consumerIp":"/{some_ip}",
   *            "topicName":"1",
   *            "partitionId":1,
   *            "currentOffset":15753823,
   *            "lag":136160,
   *            "endOffset":15889983},
   *         { "consumerId":"consumer-1-88792db6-99a2-4064-aad2-38be12b32e88",
   *            "consumerIp":"/{some_ip}",
   *            "topicName":"1",
   *            "partitionId":2,
   *            "currentOffset":15649419,
   *            "lag":133052,
   *            "endOffset":15782471}],
   *        "topicPartitionCount":3,
   *        "coordinator":{ "host":"{coordinator_host_name}","port":9496 }
   *      }
   */
  @GET
  @Path("/{groupId}/topics/{topic}")
  @PerformanceMetric("groups.get.topic.partitions")
  public ConsumerGroupSubscription getPartitionsInformationByTopic(
          @PathParam("groupId") String groupId,
          @PathParam("topic") String topic,
          @QueryParam("page_offset") Integer pageOffset,
          @QueryParam("page_size") Integer pageSize)
          throws Exception {
    final boolean needPartOfData = Optional.ofNullable(pageOffset).isPresent()
        && Optional.ofNullable(pageSize).isPresent()
        && pageSize > 0;
    if (needPartOfData) {
      return context.getGroupMetadataObserver()
          .getConsumerGroupInformation(groupId,
              Collections.singleton(topic),
              pageOffset,
              pageSize);
    }
    return context.getGroupMetadataObserver()
        .getConsumerGroupInformation(groupId,
            Collections.singleton(topic));
  }
}
