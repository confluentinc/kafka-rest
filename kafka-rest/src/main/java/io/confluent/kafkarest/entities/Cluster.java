/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Kafka cluster.
 */
@AutoValue
public abstract class Cluster {

  Cluster() {
  }

  public abstract String getClusterId();

  @Nullable
  public abstract Broker getController();

  public abstract ImmutableList<Broker> getBrokers();

  public static Builder builder() {
    return new AutoValue_Cluster.Builder();
  }

  public static Cluster create(
      String clusterId, @Nullable Broker controller, List<Broker> brokers) {
    return builder()
        .setClusterId(clusterId)
        .setController(controller)
        .addAllBrokers(brokers)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setController(Broker controller);

    abstract ImmutableList.Builder<Broker> brokersBuilder();

    public final Builder addAllBrokers(Iterable<Broker> brokers) {
      brokersBuilder().addAll(brokers);
      return this;
    }

    public abstract Cluster build();
  }
}
