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

package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Cluster;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to manage Kafka {@link Cluster Clusters}.
 */
public interface ClusterManager {

  /**
   * Returns the list of Kafka {@link Cluster Clusters} known.
   *
   * <p>Right now only one cluster is known, namely, the cluster to which this application is
   * connected to. Therefore, this method will always return at most 1 cluster.</p>
   *
   * @see #getLocalCluster()
   */
  CompletableFuture<List<Cluster>> listClusters();

  /**
   * Returns the Kafka {@link Cluster} with the given {@code clusterId}, or empty if no such cluster
   * is known.
   */
  CompletableFuture<Optional<Cluster>> getCluster(String clusterId);

  /**
   * Returns the Kafka {@link Cluster} this application is connected to.
   */
  CompletableFuture<Cluster> getLocalCluster();
}
