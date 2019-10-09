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
import scala.Option;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.HashSet;
import java.util.Comparator;


public class GroupMetadataObserver {

  private static KafkaConsumer<Object, Object> createConsumer(String groupId,
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
    this.config = config;
    this.adminClientWrapper = adminClientWrapper;
  }

  /**
   * <p>Get consumer group list</p>
   *
   * @return list of consumer groups
   */
  public List<ConsumerGroup> getConsumerGroupList(Option<Integer> offsetOpt,
                                                  Option<Integer> countOpt) throws Exception {
    if (Option.apply(adminClientWrapper).nonEmpty()) {
      final boolean needPartOfData = offsetOpt.nonEmpty()
              && countOpt.nonEmpty()
              && countOpt.get() > 0;
      Collection<ConsumerGroupListing> groupsOverview =
              adminClientWrapper.listConsumerGroups();
      if (needPartOfData) {
        final Comparator<ConsumerGroupListing> consumerGroupListingComparator =
                Comparator.comparing(ConsumerGroupListing::groupId);
        final List<ConsumerGroupListing> consumerGroupListings = new ArrayList<>(groupsOverview);
        consumerGroupListings.sort(consumerGroupListingComparator);
        groupsOverview = JavaConverters.asJavaCollection(
                JavaConverters.collectionAsScalaIterable(consumerGroupListings)
                .slice(offsetOpt.get(), offsetOpt.get() + countOpt.get()));
      }
      final List<ConsumerGroup> result = new ArrayList<>();
      for (ConsumerGroupListing eachGroupInfo : groupsOverview) {
        final Node node = adminClientWrapper.describeConsumerGroups(
                Collections.singleton(eachGroupInfo.groupId()))
                .get(eachGroupInfo.groupId()).coordinator();
        result.add(new ConsumerGroup(eachGroupInfo.groupId(),
                new ConsumerGroupCoordinator(node.host(), node.port())));
      }
      return result;
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId - group name
   * @return description of consumer group
   */
  public Set<Topic> getConsumerGroupTopicInformation(String groupId,
                                                         Option<Integer> offsetOpt,
                                                         Option<Integer> countOpt)
          throws Exception {
    if (Option.apply(adminClientWrapper).nonEmpty()) {
      final Collection<MemberDescription> summariesOpt =
              adminClientWrapper.describeConsumerGroups(Collections.singleton(groupId))
                      .get(groupId).members();
      if (summariesOpt.size() > 0) {
        final Set<Topic> result = new HashSet<>();
        for (MemberDescription eachSummary : summariesOpt) {
          for (TopicPartition topicPartition : eachSummary.assignment().topicPartitions()) {
            result.add(new Topic(topicPartition.topic(), null, null));
          }
        }
        log.debug("Get topic list {}", result);
        final boolean needPartOfData = offsetOpt.nonEmpty()
                && countOpt.nonEmpty()
                && countOpt.get() > 0;
        if (!needPartOfData) {
          return result;
        } else {
          final scala.collection.Set<Topic> slice =
                  JavaConverters.asScalaSetConverter(result).asScala()
                          .slice(offsetOpt.get(), offsetOpt.get() + countOpt.get()).toSet();
          return JavaConverters.setAsJavaSetConverter(slice).asJava();
        }
      } else {
        return Collections.emptySet();
      }
    } else {
      return Collections.emptySet();
    }
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId - group name
   * @return description of consumer group
   *     (all consumed topics with all partition offset information)
   */
  public ConsumerGroupSubscription getConsumerGroupInformation(String groupId) throws Exception {
    return getConsumerGroupInformation(groupId, Option.<String>empty(),
            Option.<Integer>empty(), Option.<Integer>empty());
  }

  /**
   * <p>Get consumer group description</p>
   *
   * @param groupId   - group name
   * @param topic     - topic name for filter - default empty topic name
   * @param offsetOpt - offset for TopicPartitionEntity
   *                  collection for each consumer member for paging
   * @param countOpt  - count of elements TopicPartitionEntity
   *                  collection for each consumer member for paging
   * @return description of consumer group
   */
  public ConsumerGroupSubscription getConsumerGroupInformation(
          String groupId,
          Option<String> topic,
          Option<Integer> offsetOpt,
          Option<Integer> countOpt) throws Exception {
    if (Option.apply(adminClientWrapper).nonEmpty()) {
      final ConsumerGroupDescription consumerGroupSummary =
              adminClientWrapper.describeConsumerGroups(Collections.singleton(groupId))
                      .get(groupId);
      final Collection<MemberDescription> summaries =
              consumerGroupSummary.members();
      if (summaries.size() > 0) {
        log.debug("Get summary list {}", summaries);
        try (KafkaConsumer kafkaConsumer = createConsumer(groupId, config)) {
          final List<ConsumerTopicPartitionDescription> consumerTopicPartitionDescriptions =
              getConsumerTopicPartitionDescriptions(topic, summaries, kafkaConsumer);
          final Comparator<ConsumerTopicPartitionDescription> confluentTopicPartitionComparator =
              Comparator.comparingInt(ConsumerTopicPartitionDescription::getPartitionId);
          consumerTopicPartitionDescriptions.sort(confluentTopicPartitionComparator);
          final Node coordinatorNode = consumerGroupSummary.coordinator();
          return new ConsumerGroupSubscription(
            getPagedTopicPartitionList(consumerTopicPartitionDescriptions, offsetOpt, countOpt),
            consumerTopicPartitionDescriptions.size(),
            new ConsumerGroupCoordinator(coordinatorNode.host(), coordinatorNode.port()));
        }
      } else {
        return ConsumerGroupSubscription.empty();
      }
    } else {
      return ConsumerGroupSubscription.empty();
    }
  }

  private List<ConsumerTopicPartitionDescription> getConsumerTopicPartitionDescriptions(
          Option<String> topic,
          Collection<MemberDescription> consumerGroupMembers,
          KafkaConsumer kafkaConsumer) {
    final List<ConsumerTopicPartitionDescription> consumerTopicPartitionDescriptions =
            new ArrayList<>();
    for (MemberDescription summary : consumerGroupMembers) {
      final Comparator<TopicPartition> topicPartitionComparator =
              Comparator.comparingInt(TopicPartition::partition);
      final List<TopicPartition> filteredTopicPartitions =
              new ArrayList<>(summary.assignment().topicPartitions());
      if (topic.nonEmpty()) {
        final List<TopicPartition> newTopicPartitions = new ArrayList<>();
        for (TopicPartition topicPartition : filteredTopicPartitions) {
          if (topicPartition.topic().equals(topic.get())) {
            newTopicPartitions.add(topicPartition);
          }
        }
        filteredTopicPartitions.addAll(newTopicPartitions);
      }
      filteredTopicPartitions.sort(topicPartitionComparator);
      kafkaConsumer.assign(filteredTopicPartitions);
      for (TopicPartition topicPartition : filteredTopicPartitions) {
        final OffsetAndMetadata metadata = kafkaConsumer.committed(topicPartition);
        // Get current offset
        final Long currentOffset = Option.apply(metadata).nonEmpty() ? metadata.offset() : 0;
        // Goto end offset for current TopicPartition WITHOUT COMMIT
        kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
        // Get end offset
        final Long totalOffset = kafkaConsumer.position(topicPartition);

        consumerTopicPartitionDescriptions.add(
            new ConsumerTopicPartitionDescription(summary.consumerId(),
              summary.host(),
              topicPartition.topic(),
              topicPartition.partition(),
              currentOffset,
              totalOffset - currentOffset,
              totalOffset
            ));
      }
    }
    return consumerTopicPartitionDescriptions;
  }

  private List<ConsumerTopicPartitionDescription> getPagedTopicPartitionList(
          List<ConsumerTopicPartitionDescription> topicPartitionList,
          Option<Integer> offsetOpt,
          Option<Integer> countOpt) {
    final boolean needPartOfData = offsetOpt.nonEmpty()
            && countOpt.nonEmpty()
            && countOpt.get() > 0;
    if (needPartOfData) {
      return JavaConverters.seqAsJavaListConverter(
              JavaConverters.iterableAsScalaIterableConverter(topicPartitionList)
                      .asScala()
                      .slice(offsetOpt.get(), offsetOpt.get() + countOpt.get())
                      .toList())
              .asJava();
    } else {
      return topicPartitionList;
    }
  }
}
