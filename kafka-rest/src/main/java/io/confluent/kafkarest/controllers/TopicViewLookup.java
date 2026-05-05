/*
 * Copyright 2026 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.entities.TopicView;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Looks up topic-view metadata for the topics in a cluster. Used by {@link TopicManagerImpl} to
 * populate the {@code views} field on listed topics.
 */
public interface TopicViewLookup {

  /**
   * Returns a mapping from <em>source-topic name</em> to its derived views. Source topics with no
   * derived views may be omitted.
   */
  CompletableFuture<Map<String, ImmutableList<TopicView>>> lookupViewsBySource(String clusterId);

  /** Default no-op implementation: returns an empty map. */
  TopicViewLookup NO_OP = clusterId -> CompletableFuture.completedFuture(ImmutableMap.of());
}
