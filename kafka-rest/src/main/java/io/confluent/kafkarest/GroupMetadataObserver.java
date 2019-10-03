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

import io.confluent.kafkarest.entities.ConsumerEntity;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroupCoordinator;
import io.confluent.kafkarest.entities.TopicName;
import io.confluent.kafkarest.entities.TopicPartitionEntity;
import org.apache.kafka.clients.admin.AdminClient;
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
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;


public class GroupMetadataObserver {

  private static AdminClient createAdminClient(KafkaRestConfig appConfig) {
    return AdminClient.create(AdminClientWrapper.adminProperties(appConfig));
  }

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
  private final AdminClient adminClient;
  private final int initTimeOut;

  public GroupMetadataObserver(KafkaRestConfig config) {
    this.config = config;
    this.adminClient = createAdminClient(config);
    this.initTimeOut = config.getInt(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG);
  }

  /**
   * <p>Get consumer group list</p>
   *
   * @return list of consumer groups
   */
  public List<ConsumerGroup> getConsumerGroupList(Option<Integer> offsetOpt,
                                                  Option<Integer> countOpt) throws Exception {
    if (Option.apply(adminClient).nonEmpty()) {
      final boolean needPartOfData = offsetOpt.nonEmpty()
              && countOpt.nonEmpty()
              && countOpt.get() > 0;
      Collection<ConsumerGroupListing> groupsOverview =
              adminClient.listConsumerGroups().all().get(initTimeOut, TimeUnit.MILLISECONDS);
      if (needPartOfData) {
        final Comparator<ConsumerGroupListing> consumerGroupListingComparator =
                Comparator.comparing(ConsumerGroupListing::groupId);
        List<ConsumerGroupListing> consumerGroupListings = new ArrayList<>(groupsOverview);
        consumerGroupListings.sort(consumerGroupListingComparator);
        groupsOverview = JavaConverters.asJavaCollection(
                JavaConverters.collectionAsScalaIterable(consumerGroupListings)
                .slice(offsetOpt.get(), offsetOpt.get() + countOpt.get()));
      }
      final List<ConsumerGroup> result = new ArrayList<>();
      for (ConsumerGroupListing eachGroupInfo : groupsOverview) {
        final Node node = adminClient.describeConsumerGroups(
                Collections.singleton(eachGroupInfo.groupId()))
                .all().get(initTimeOut, TimeUnit.MILLISECONDS)
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
  public Set<TopicName> getConsumerGroupTopicInformation(String groupId,
                                                         Option<Integer> offsetOpt,
                                                         Option<Integer> countOpt)
          throws Exception {
    if (Option.apply(adminClient).nonEmpty()) {
      final Collection<MemberDescription> summariesOpt =
              adminClient.describeConsumerGroups(Collections.singleton(groupId))
                      .all().get(initTimeOut, TimeUnit.MILLISECONDS).get(groupId).members();
      if (summariesOpt.size() > 0) {
        final Set<TopicName> result = new HashSet<>();
        for (MemberDescription eachSummary : summariesOpt) {
          for (TopicPartition topicPartition : eachSummary.assignment().topicPartitions()) {
            result.add(new TopicName(topicPartition.topic()));
          }
        }
        log.debug("Get topic list {}", result);
        final boolean needPartOfData = offsetOpt.nonEmpty()
                && countOpt.nonEmpty()
                && countOpt.get() > 0;
        if (!needPartOfData) {
          return result;
        } else {
          final scala.collection.Set<TopicName> slice =
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
  public ConsumerEntity getConsumerGroupInformation(String groupId) throws Exception {
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
  public ConsumerEntity getConsumerGroupInformation(String groupId, Option<String> topic,
                                                    Option<Integer> offsetOpt,
                                                    Option<Integer> countOpt) throws Exception {
    if (Option.apply(adminClient).nonEmpty()) {
      final ConsumerGroupDescription consumerGroupSummary =
              adminClient.describeConsumerGroups(Collections.singleton(groupId))
                      .all().get(initTimeOut, TimeUnit.MILLISECONDS).get(groupId);
      final Collection<MemberDescription> summaries =
              consumerGroupSummary.members();
      if (summaries.size() > 0) {
        log.debug("Get summary list {}", summaries);
        try (KafkaConsumer kafkaConsumer = createConsumer(groupId, config)) {
          final List<TopicPartitionEntity> confluentTopicPartitions = new ArrayList<>();
          for (MemberDescription summary : summaries) {
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

              confluentTopicPartitions.add(new TopicPartitionEntity(summary.consumerId(),
                      summary.host(),
                      topicPartition.topic(),
                      topicPartition.partition(),
                      currentOffset,
                      totalOffset - currentOffset,
                      totalOffset
              ));
            }
          }
          final Comparator<TopicPartitionEntity> confluentTopicPartitionComparator =
                  Comparator.comparingInt(TopicPartitionEntity::getPartitionId);
          confluentTopicPartitions.sort(confluentTopicPartitionComparator);
          final Node coordinatorNode = consumerGroupSummary.coordinator();
          return new ConsumerEntity(
                  getPagedTopicPartitionList(confluentTopicPartitions, offsetOpt, countOpt),
                  confluentTopicPartitions.size(),
                  new ConsumerGroupCoordinator(coordinatorNode.host(), coordinatorNode.port()));
        }
      } else {
        return ConsumerEntity.empty();
      }
    } else {
      return ConsumerEntity.empty();
    }
  }

  /**
   * <p>Shutdown observer</p>
   */
  public void shutdown() {
    log.debug("Shutting down MetadataObserver");
    adminClient.close();
  }

  private List<TopicPartitionEntity> getPagedTopicPartitionList(
          List<TopicPartitionEntity> topicPartitionList,
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
