/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.CopyOnWriteArrayList;

final class Replica {
  private final int brokerId;
  private final CopyOnWriteArrayList<Record> records = new CopyOnWriteArrayList<>();

  Replica(int brokerId) {
    this.brokerId = brokerId;
  }

  int getBrokerId() {
    return brokerId;
  }

  ImmutableList<Record> getRecords() {
    return ImmutableList.copyOf(records);
  }

  synchronized int addRecord(Record record) {
    int offset = records.size();
    records.add(record);
    return offset;
  }
}
