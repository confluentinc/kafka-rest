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

import java.util.List;

import io.confluent.kafkarest.entities.Topic;
import kafka.cluster.Broker;

public class UnsupportedMetaDataObserver extends MetadataObserver {

  public UnsupportedMetaDataObserver() {
    super(null);
  }

  @Override
  public Broker getLeader(String topicName, int partitionId) {
    throw new UnsupportedOperationException("Can't perform operation without zookeeper connect "
                                            + "details");
  }

  @Override
  public List<Topic> getTopics() {
    throw new UnsupportedOperationException("Can't perform operation without zookeeper connect "
                                            + "details");
  }

  @Override
  public boolean topicExists(String topicName) {
    throw new UnsupportedOperationException("Can't perform operation without zookeeper connect "
                                            + "details");
  }

}
