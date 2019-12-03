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
package io.confluent.kafkarest.v2;

import static java.util.Collections.unmodifiableMap;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

public class MockConsumer<K, V> extends org.apache.kafka.clients.consumer.MockConsumer<K, V> {
    private String cid;
    String groupName;

    private final Map<TopicPartition, SortedSet<OffsetAndTimestamp>> offsetForTimes =
        new HashMap<>();

    MockConsumer(OffsetResetStrategy offsetResetStrategy, String groupName) {
        super(offsetResetStrategy);
        this.groupName = groupName;
    }

    public String cid() {
        return cid;
    }

    public void cid(String cid) {
        this.cid = cid;
    }

    synchronized void updateOffsetForTime(
        String topic, int partition, long offset, Instant timestamp) {
        SortedSet<OffsetAndTimestamp> offsets =
            offsetForTimes.computeIfAbsent(
                new TopicPartition(topic, partition),
                key -> new TreeSet<>(Comparator.comparing(OffsetAndTimestamp::timestamp)));
        offsets.add(new OffsetAndTimestamp(offset, timestamp.toEpochMilli()));
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
        Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
        for (Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            SortedSet<OffsetAndTimestamp> offsets =
                offsetForTimes.getOrDefault(entry.getKey(), new TreeSet<>());
            SortedSet<OffsetAndTimestamp> tail =
                offsets.tailSet(new OffsetAndTimestamp(0L, entry.getValue()));
            if (tail.isEmpty()) {
                result.put(entry.getKey(), /* value= */ null);
            } else {
                result.put(entry.getKey(), tail.first());
            }
        }
        return unmodifiableMap(result);
    }
}
