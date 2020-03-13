/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import kafka.api.LeaderAndIsr;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.Seq;
import scala.math.Ordering;

/**
 * Observes metadata about the Kafka cluster.
 */
@Deprecated
public class MetadataObserver {

  private static final Logger log = LoggerFactory.getLogger(MetadataObserver.class);

  private ZkUtils zkUtils;

  public MetadataObserver(ZkUtils zkUtils) {
    this.zkUtils = zkUtils;
  }


  public Broker getLeader(final String topicName, final int partitionId) {
    return getBrokerById(getLeaderId(topicName, partitionId));
  }

  public boolean topicExists(String topicName) {
    Collection<String> topicNames = getTopicNames();
    for (String topic : topicNames) {
      if (topic.equals(topicName)) {
        return true;
      }
    }
    return false;
  }

  private Collection<String> getTopicNames() {
    Seq<String> topicNames = zkUtils.getAllTopics().sorted(Ordering.String$.MODULE$);
    return JavaConversions.asJavaCollection(topicNames);
  }

  private int getLeaderId(final String topicName, final int partitionId) {
    final List<Partition> partitions = getTopicPartitions(topicName);

    if (partitions.size() == 0) {
      throw Errors.topicNotFoundException();
    }

    for (final Partition partition : partitions) {
      if (partition.getPartitionId() == partitionId) {
        return partition.getLeader().map(PartitionReplica::getBrokerId).orElse(-1);
      }
    }

    throw Errors.partitionNotFoundException();
  }

  private Broker getBrokerById(final int brokerId) {
    Option<Broker> broker = zkUtils.getBrokerInfo(brokerId);

    if (broker.isDefined()) {
      return broker.get();
    } else {
      throw Errors.leaderNotAvailableException();
    }
  }

  private List<Partition> getTopicPartitions(String topic) {
    return getTopicPartitions(topic, null);
  }

  private List<Partition> getTopicPartitions(String topic, Integer partitionsFilter) {
    Map<String, Map<Object, Seq<Object>>> topicPartitions = zkUtils.getPartitionAssignmentForTopics(
        JavaConversions.asScalaBuffer(Arrays.asList(topic)));
    if (!topicPartitions.get(topic).isEmpty()) {
      Map<Object, Seq<Object>> parts = topicPartitions.get(topic).get();
      return extractPartitionsFromZkData(parts, topic, partitionsFilter);
    }
    return null;
  }

  private List<Partition> extractPartitionsFromZkData(
      Map<Object, Seq<Object>> parts,
      String topic,
      Integer partitionsFilter
  ) {
    List<Partition> partitions = new Vector<Partition>();
    java.util.Map<Object, Seq<Object>> partsJava = JavaConversions.mapAsJavaMap(parts);
    for (java.util.Map.Entry<Object, Seq<Object>> part : partsJava.entrySet()) {
      int partId = (Integer) part.getKey();
      if (partitionsFilter != null && partitionsFilter != partId) {
        continue;
      }

      Option<LeaderAndIsr> leaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(topic, partId);
      if (!leaderAndIsrOpt.isEmpty()) {
        LeaderAndIsr leaderAndIsr = leaderAndIsrOpt.get();
        scala.collection.immutable.Set<Integer> isr = leaderAndIsr.isr().toSet();
        List<PartitionReplica> partReplicas = new Vector<PartitionReplica>();
        for (Object brokerObj : JavaConversions.asJavaCollection(part.getValue())) {
          int broker = (Integer) brokerObj;
          PartitionReplica r =
              new PartitionReplica(
                  /* clusterId= */ "",
                  topic,
                  partId,
                  broker,
                  (leaderAndIsr.leader() == broker),
                  isr.contains(broker));
          partReplicas.add(r);
        }
        Partition p = new Partition(/* clusterId= */ "", topic, partId, partReplicas);
        partitions.add(p);
      }
    }
    return partitions;
  }

  public void shutdown() {
    log.debug("Shutting down MetadataObserver");
  }
}
