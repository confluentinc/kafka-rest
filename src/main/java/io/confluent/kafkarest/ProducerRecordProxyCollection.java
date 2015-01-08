/**
 * Copyright 2015 Confluent Inc.
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
 **/
package io.confluent.kafkarest;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Iterator;

import io.confluent.kafkarest.entities.ProduceRecord;

/**
 * Implementation of Iterable<ProducerRecord> that just wraps an underlying collection of
 * ProduceRecord entities.
 */
public class ProducerRecordProxyCollection implements Iterable<ProducerRecord> {

  private final String topic;
  private final Integer partition;
  private final Collection<? extends ProduceRecord> underlying;

  public ProducerRecordProxyCollection(String topic, Collection<? extends ProduceRecord> records) {
    this.topic = topic;
    this.partition = null;
    this.underlying = records;
  }

  public ProducerRecordProxyCollection(String topic, int partition,
                                       Collection<ProduceRecord> records) {
    this.topic = topic;
    this.partition = partition;
    this.underlying = records;
  }

  public int size() {
    return underlying.size();
  }

  @Override
  public Iterator<ProducerRecord> iterator() {
    return new ProxyIterator(underlying.iterator());
  }

  private class ProxyIterator implements Iterator<ProducerRecord> {

    Iterator<? extends ProduceRecord> underlying;

    public ProxyIterator(Iterator<? extends ProduceRecord> underlying) {
      this.underlying = underlying;
    }

    @Override
    public boolean hasNext() {
      return underlying.hasNext();
    }

    @Override
    public ProducerRecord next() {
      if (partition == null) {
        return underlying.next().getKafkaRecord(topic);
      } else {
        return underlying.next().getKafkaRecord(topic, partition);
      }
    }

    @Override
    public void remove() {
      underlying.remove();
    }
  }
}
