/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * httcp://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroupCoordinator;
import io.confluent.kafkarest.entities.ConsumerGroupSubscription;
import io.confluent.kafkarest.entities.ConsumerTopicPartitionDescription;
import io.confluent.kafkarest.entities.Topic;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.stream.Collectors;


public class GroupMetadataObserver {

  private static KafkaConsumer<?, ?> createConsumer(String groupId,
                                                    KafkaRestConfig appConfig) {
    final Properties properties = new Properties();
    String deserializer = StringDeserializer.class.getName();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            RestConfigUtils.bootstrapBrokers(appConfig));
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
    properties.putAll(appConfig.getConsumerProperties());

    return new KafkaConsumer<>(properties);
  }

  private final Logger log = LoggerFactory.getLogger(GroupMetadataObserver.class);

  private final KafkaRestConfig config;
  private final AdminClientWrapper adminClientWrapper;

  public GroupMetadataObserver(KafkaRestConfig config, AdminClientWrapper adminClientWrapper) {
    this.config = Objects.requireNonNull(config);
    this.adminClientWrapper = Objects.requireNonNull(adminClientWrapper);
  }

  /**
   * <p>Get consumer group list restricted by paging parameters</p>
   *
   * @return list of consumer groups
   */
  public List<ConsumerGroup> getPagedConsumerGroupList(Integer startPos,
                                                       Integer count)
      throws Exception {
    return getConsumerGroups(getPagedConsumerGroup(startPos, count));
  }

  /**
   * <p>Get consumer group list</p>
   *
   * @return list of consumer groups
   */
  public List<ConsumerGroup> getConsumerGroupList() throws Exception {
    return getConsumerGroups(adminClientWrapper.listConsumerGroups());
  }

  private List<ConsumerGroup> getConsumerGroups(Collection<ConsumerGroupListing> groupsOverview)
      throws Exception {
    final List<ConsumerGroup> result = new ArrayList<>();
    List<String> groupIds = groupsOverview.stream().map(ConsumerGroupListing::groupId)
        .collect(Collectors.toList());
    for (Entry<String, ConsumerGroupDescription> eachGroupInfo :
            adminClientWrapper.describeConsumerGroups(groupIds).entrySet()) {
      final Node node = eachGroupInfo.getValue().coordinator();
      result.add(new ConsumerGroup(eachGroupInfo.getKey(),
          new ConsumerGroupCoordinator(node.host(), node.port())));
    }
    return result;
  }

  private Collection<ConsumerGroupListing> getPagedConsumerGroup(Integer startPosition,
                                                                 Integer count)
      throws Exception {
    Collection<ConsumerGroupListing> groupsOverview;
    final List<ConsumerGroupListing> consumerGroupListings =
        new ArrayList<>(adminClientWrapper.listConsumerGroups());
    consumerGroupListings.sort(Comparator.comparing(ConsumerGroupListing::groupId));
    groupsOverview = consumerGroupListings.subList(startPosition,
        Math.min(consumerGroupListings.size(), startPosition + count));
    return groupsOverview;
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId - group name
   * @return description of consumer group
   */
  public Set<Topic> getConsumerGroupTopicInformation(String groupId)
          throws Exception {
    final Set<Topic> result = getConsumerGroupTopics(groupId);
    log.debug("Get topic list {}", result);
    return result;
  }

  /**
   * <p>Get consumer group description restricted by paging parameters</p>
   *
   * @param groupId - group name
   * @return description of consumer group
   */
  public Set<Topic> getPagedConsumerGroupTopicInformation(String groupId,
                                                          Integer startPos,
                                                          Integer count)
          throws Exception {
    final Set<Topic> result = getConsumerGroupTopics(groupId);
    log.debug("Get topic list {}", result);
    return result.stream()
        .skip(startPos)
        .limit(Math.min(result.size(), startPos + count))
        .collect(Collectors.toSet());
  }

  private Set<Topic> getConsumerGroupTopics(String groupId) throws Exception {
    final Set<Topic> result = new HashSet<>();
    final Collection<MemberDescription> memberDescriptions =
        adminClientWrapper.describeConsumerGroups(Collections.singleton(groupId))
            .get(groupId).members();
    if (memberDescriptions.isEmpty()) {
      return Collections.emptySet();
    }
    for (MemberDescription eachSummary : memberDescriptions) {
      for (TopicPartition topicPartition : eachSummary.assignment().topicPartitions()) {
        result.add(new Topic(topicPartition.topic(), null, null));
      }
    }
    return result;
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId - group name
   * @return description of consumer group
   *     (all consumed topics with all partition offset information)
   */
  public ConsumerGroupSubscription getConsumerGroupInformation(String groupId) throws Exception {
    return getConsumerGroupInformation(groupId, Collections.emptyList());
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId   - group name
   * @param topics    - topic names for filter - default empty topic names
   * @param offsetOpt - offset for TopicPartitionEntity
   *                  collection for each consumer member for paging
   * @param countOpt  - count of elements TopicPartitionEntity
   *                  collection for each consumer member for paging
   * @return description of consumer group
   */
  public ConsumerGroupSubscription getConsumerGroupInformation(
          String groupId,
          Collection<String> topics,
          Integer offsetOpt,
          Integer countOpt) throws Exception {
    final ConsumerGroupDescription consumerGroupSummary =
        adminClientWrapper.describeConsumerGroups(Collections.singleton(groupId))
            .get(groupId);
    final Collection<MemberDescription> summaries = consumerGroupSummary.members();
    if (summaries.isEmpty()) {
      return ConsumerGroupSubscription.empty();
    }
    log.debug("Get summary list {}", summaries);
    try (KafkaConsumer kafkaConsumer = createConsumer(groupId, config)) {
      final List<ConsumerTopicPartitionDescription> consumerTopicPartitionDescriptions =
          getConsumerTopicPartitionDescriptions(topics, summaries, kafkaConsumer);
      final Node coordinatorNode = consumerGroupSummary.coordinator();
      return new ConsumerGroupSubscription(
          getPagedTopicPartitionList(consumerTopicPartitionDescriptions, offsetOpt, countOpt),
          consumerTopicPartitionDescriptions.size(),
          new ConsumerGroupCoordinator(coordinatorNode.host(), coordinatorNode.port()));
    }
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId   - group name
   * @param topics    - topic names for filter - default empty topic names
   * @return description of consumer group
   */
  public ConsumerGroupSubscription getConsumerGroupInformation(
          String groupId,
          Collection<String> topics) throws Exception {
    final ConsumerGroupDescription consumerGroupSummary =
        adminClientWrapper.describeConsumerGroups(Collections.singleton(groupId))
            .get(groupId);
    final Collection<MemberDescription> summaries = consumerGroupSummary.members();
    if (summaries.isEmpty()) {
      return ConsumerGroupSubscription.empty();
    }
    log.debug("Get summary list {}", summaries);
    try (KafkaConsumer kafkaConsumer = createConsumer(groupId, config)) {
      final List<ConsumerTopicPartitionDescription> consumerTopicPartitionDescriptions =
          getConsumerTopicPartitionDescriptions(topics, summaries, kafkaConsumer);
      final Node coordinatorNode = consumerGroupSummary.coordinator();
      return new ConsumerGroupSubscription(
          consumerTopicPartitionDescriptions,
          consumerTopicPartitionDescriptions.size(),
          new ConsumerGroupCoordinator(coordinatorNode.host(), coordinatorNode.port()));
    }
  }

  private List<ConsumerTopicPartitionDescription> getConsumerTopicPartitionDescriptions(
          Collection<String> topics,
          Collection<MemberDescription> consumerGroupMembers,
          KafkaConsumer kafkaConsumer) {
    final List<ConsumerTopicPartitionDescription> consumerTopicPartitionDescriptions =
            new ArrayList<>();
    for (MemberDescription summary : consumerGroupMembers) {
      final Set<TopicPartition> assignedTopicPartitions =
                summary.assignment().topicPartitions();
      final List<TopicPartition> filteredTopicPartitions = new ArrayList<>();
      if (!topics.isEmpty()) {
        final List<TopicPartition> newTopicPartitions = new ArrayList<>();
        for (TopicPartition topicPartition : assignedTopicPartitions) {
          if (topics.contains(topicPartition.topic())) {
            newTopicPartitions.add(topicPartition);
          }
        }
        filteredTopicPartitions.addAll(newTopicPartitions);
      } else {
        filteredTopicPartitions.addAll(assignedTopicPartitions);
      }
      filteredTopicPartitions.sort(Comparator.comparingInt(TopicPartition::partition));
      kafkaConsumer.assign(filteredTopicPartitions);
      consumerTopicPartitionDescriptions.addAll(
          createConsumerTopicPartitionDescriptions(kafkaConsumer,
              summary, filteredTopicPartitions));
    }
    consumerTopicPartitionDescriptions.sort(
        Comparator.comparingInt(ConsumerTopicPartitionDescription::getPartitionId));
    return consumerTopicPartitionDescriptions;
  }

  private List<ConsumerTopicPartitionDescription> createConsumerTopicPartitionDescriptions(
      KafkaConsumer kafkaConsumer,
      MemberDescription summary,
      List<TopicPartition> filteredTopicPartitions) {
    final List<ConsumerTopicPartitionDescription> result = new ArrayList<>();
    for (TopicPartition topicPartition : filteredTopicPartitions) {
      final OffsetAndMetadata metadata = kafkaConsumer.committed(topicPartition);
      // Get current offset
      final Long currentOffset = Optional.ofNullable(metadata).isPresent()
          ? metadata.offset() : 0;
      // Goto end offset for current TopicPartition WITHOUT COMMIT
      kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
      // Get end offset
      final Long totalOffset = kafkaConsumer.position(topicPartition);
      result.add(
          new ConsumerTopicPartitionDescription(summary.consumerId(),
            summary.host(),
            topicPartition.topic(),
            topicPartition.partition(),
            currentOffset,
            totalOffset - currentOffset,
            totalOffset
          ));
    }
    return result;
  }

  private List<ConsumerTopicPartitionDescription> getPagedTopicPartitionList(
          List<ConsumerTopicPartitionDescription> topicPartitionList,
          Integer offsetOpt,
          Integer countOpt) {
    return topicPartitionList.subList(offsetOpt,
        Math.min(topicPartitionList.size(), offsetOpt + countOpt));
  }
}
