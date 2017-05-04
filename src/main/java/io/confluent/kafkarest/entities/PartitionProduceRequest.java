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

import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class PartitionProduceRequest<RecordT extends ProduceRecord> extends SchemaHolder {

  @NotEmpty
  private List<RecordT> records;

  @JsonProperty
  public List<RecordT> getRecords() {
    return records;
  }

  @JsonProperty
  public void setRecords(List<RecordT> records) {
    this.records = records;
  }

  @Override
  public String toString() {
    return "PartitionProduceRequest{" + "records=" + records + '}';
  }
}
