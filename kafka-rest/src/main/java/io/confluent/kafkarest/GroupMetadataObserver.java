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

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ConsumerEntity;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroupCoordinator;
import io.confluent.kafkarest.entities.TopicName;
import io.confluent.kafkarest.entities.TopicPartitionEntity;
import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.clients.CommonClientConfigs;
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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;


public class GroupMetadataObserver {

  private static AdminClient createAdminClient(KafkaRestConfig appConfig) {
    final Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, appConfig.bootstrapBrokers());
    properties.putAll(appConfig.getConsumerProperties());
    return AdminClient.create(properties);
  }

  private static KafkaConsumer<Object, Object> createConsumer(String groupId,
                                                              KafkaRestConfig appConfig) {
    final Properties properties = new Properties();
    String deserializer = StringDeserializer.class.getName();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.bootstrapBrokers());
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

  public GroupMetadataObserver(KafkaRestConfig config) {
    this.config = config;
    this.adminClient = createAdminClient(config);
  }

  /**
   * <p>Get consumer group list</p>
   *
   * @return list of consumer groups
   */
  public List<ConsumerGroup> getConsumerGroupList(Option<Integer> offsetOpt,
                                                  Option<Integer> countOpt) {
    if (Option.apply(adminClient).nonEmpty()) {
      final Boolean needPartOfData = offsetOpt.nonEmpty()
              && countOpt.nonEmpty()
              && countOpt.get() > 0;
      scala.collection.immutable.List<GroupOverview> groupsOverview =
              adminClient.listAllConsumerGroupsFlattened();
      if (needPartOfData) {
        groupsOverview = groupsOverview.slice(offsetOpt.get(), offsetOpt.get() + countOpt.get());
      }
      final List<ConsumerGroup> result = new ArrayList<>();
      for (GroupOverview eachGroupInfo :
              JavaConverters.seqAsJavaListConverter(groupsOverview).asJava()) {
        final Node node = adminClient.findCoordinator(eachGroupInfo.groupId(), 0);
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
                                                         Option<Integer> countOpt) {
    if (Option.apply(adminClient).nonEmpty()) {
      final Option<scala.collection.immutable.List<ConsumerSummary>> summariesOpt =
              adminClient.describeConsumerGroup(groupId, 0).consumers();
      if (summariesOpt.nonEmpty()) {
        final Set<TopicName> result = new HashSet<>();
        for (ConsumerSummary eachSummary :
                JavaConverters.seqAsJavaListConverter(summariesOpt.get()).asJava()) {
          for (TopicPartition topicPartition :
                  JavaConverters.seqAsJavaListConverter(eachSummary.assignment()).asJava()) {
            result.add(new TopicName(topicPartition.topic()));
          }
        }
        log.debug("Get topic list {}", result);
        final Boolean needPartOfData = offsetOpt.nonEmpty()
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
  public ConsumerEntity getConsumerGroupInformation(String groupId) {
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
                                                    Option<Integer> countOpt) {
    if (Option.apply(adminClient).nonEmpty()) {
      final ConsumerGroupSummary consumerGroupSummary =
              adminClient.describeConsumerGroup(groupId, 0);
      final Option<scala.collection.immutable.List<ConsumerSummary>> summariesOpt =
              consumerGroupSummary.consumers();
      if (summariesOpt.nonEmpty()) {
        log.debug("Get summary list {}", summariesOpt);
        try (KafkaConsumer kafkaConsumer = createConsumer(groupId, config)) {
          final List<TopicPartitionEntity> confluentTopicPartitions = new ArrayList<>();
          for (ConsumerSummary summary :
                  JavaConverters.asJavaIterableConverter(summariesOpt.get()).asJava()) {
            final Comparator<TopicPartition> topicPartitionComparator =
                new Comparator<TopicPartition>() {
                  @Override
                  public int compare(TopicPartition o1, TopicPartition o2) {
                    return o1.partition() - o2.partition();
                  }
                };
            final List<TopicPartition> filteredTopicPartitions =
                    new ArrayList<>(JavaConverters
                            .seqAsJavaListConverter(summary.assignment()).asJava());
            if (topic.nonEmpty()) {
              final List<TopicPartition> newTopicPartitions = new ArrayList<>();
              for (TopicPartition topicPartition : filteredTopicPartitions) {
                if (topicPartition.topic().equals(topic.get())) {
                  newTopicPartitions.add(topicPartition);
                }
              }
              filteredTopicPartitions.addAll(newTopicPartitions);
            }
            Collections.sort(filteredTopicPartitions, topicPartitionComparator);
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
              new Comparator<TopicPartitionEntity>() {
                @Override
                public int compare(TopicPartitionEntity o1,
                                   TopicPartitionEntity o2) {
                  return o1.getPartitionId() - o2.getPartitionId();
                }
              };
          Collections.sort(confluentTopicPartitions, confluentTopicPartitionComparator);
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
    final Boolean needPartOfData = offsetOpt.nonEmpty()
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
