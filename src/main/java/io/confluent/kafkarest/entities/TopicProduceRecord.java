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

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

import javax.validation.constraints.Min;

public class TopicProduceRecord extends ProduceRecord {

  // When producing to a topic, a partition may be explicitly requested.
  @Min(0)
  Integer partition;

  public TopicProduceRecord(@JsonProperty("key") String key, @JsonProperty("value") String value,
                            @JsonProperty("partition") Integer partition) throws IOException {
    super(key, value);
    this.partition = partition;
  }

  public TopicProduceRecord(String key, String value) throws IOException {
    super(key, value);
    this.partition = null;
  }

  public TopicProduceRecord(byte[] key, byte[] value, Integer partition) {
    super(key, value);
    this.partition = partition;
  }

  public TopicProduceRecord(byte[] key, byte[] value) {
    super(key, value);
    this.partition = null;
  }

  public TopicProduceRecord(String value, Integer partition) throws IOException {
    super(null, value);
    this.partition = partition;
  }

  public TopicProduceRecord(String value) throws IOException {
    super(null, value);
    this.partition = null;
  }

  public TopicProduceRecord(byte[] unencoded_value, Integer partition) {
    super(unencoded_value);
    this.partition = partition;
  }

  public TopicProduceRecord(byte[] unencoded_value) {
    super(unencoded_value);
    this.partition = null;
  }

  @JsonProperty
  public Integer getPartition() {
    return partition;
  }

  @JsonProperty
  public void setPartition(Integer partition) {
    this.partition = partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TopicProduceRecord that = (TopicProduceRecord) o;

    if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (partition != null ? partition.hashCode() : 0);
    return result;
  }

  @Override
  public ProducerRecord getKafkaRecord(String topic) {
    return new ProducerRecord(topic, partition, this.getKey(), this.getValue());
  }

  @Override
  public ProducerRecord getKafkaRecord(String topic, int partition) {
    throw new UnsupportedOperationException(
        "TopicProduceRecord cannot generate Kafka records for specific partitions.");
  }
}
