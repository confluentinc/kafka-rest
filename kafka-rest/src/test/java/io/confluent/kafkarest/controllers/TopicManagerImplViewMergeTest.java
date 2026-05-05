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

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicView;
import io.confluent.kafkarest.entities.v3.TopicType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TopicManagerImpl#mergeViewsAndFilter(List, Map, TopicType)}. Lives in its
 * own test class (no EasyMock extension) so the tests are independent of the admin-client mocking
 * setup in {@code TopicManagerImplTest}, which suffers from a sealed-class proxying issue with
 * newer kafka-clients versions.
 */
public class TopicManagerImplViewMergeTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String SOURCE_TOPIC_NAME = "topic-1";
  private static final String VIEW_TOPIC_NAME = "topic-2";
  private static final String REGULAR_TOPIC_NAME = "topic-3";

  /** Plain non-view topic; the lookup map will mark it as having a derived view. */
  private static final Topic SOURCE_TOPIC =
      Topic.create(
          CLUSTER_ID,
          SOURCE_TOPIC_NAME,
          ImmutableList.of(),
          (short) 3,
          /* isInternal= */ false,
          emptySet());

  /** Plain non-view topic; becomes the view of {@link #SOURCE_TOPIC} via the lookup map. */
  private static final Topic VIEW_TOPIC =
      Topic.create(
          CLUSTER_ID,
          VIEW_TOPIC_NAME,
          ImmutableList.of(),
          (short) 3,
          /* isInternal= */ false,
          emptySet());

  /** Plain non-view topic; unrelated to the view. */
  private static final Topic REGULAR_TOPIC =
      Topic.create(
          CLUSTER_ID,
          REGULAR_TOPIC_NAME,
          ImmutableList.of(),
          (short) 3,
          /* isInternal= */ false,
          emptySet());

  private static final TopicView VIEW_OF_SOURCE =
      TopicView.builder()
          .setViewTopicName(VIEW_TOPIC_NAME)
          .setViewTopicId("view-id-2")
          .setViewFilter("SELECT * FROM `" + SOURCE_TOPIC_NAME + "`")
          .setFlinkStatementId("stmt-1")
          .setFlinkComputePoolId("pool-1")
          .setViewStatus("ACTIVE")
          .setViewStatusMessage(null)
          .setStatusChangedAt(1714780823000L)
          .build();

  /** Lookup map: SOURCE_TOPIC has one derived view (VIEW_TOPIC). */
  private static final Map<String, ImmutableList<TopicView>> ONE_VIEW_LOOKUP =
      ImmutableMap.of(SOURCE_TOPIC_NAME, ImmutableList.of(VIEW_OF_SOURCE));

  private static final List<Topic> THREE_TOPICS =
      Arrays.asList(SOURCE_TOPIC, VIEW_TOPIC, REGULAR_TOPIC);

  @Test
  public void all_attachesViewsToSourceAndKeepsAllTopics() {
    List<Topic> topics =
        TopicManagerImpl.mergeViewsAndFilter(THREE_TOPICS, ONE_VIEW_LOOKUP, TopicType.ALL);

    assertEquals(3, topics.size());
    Topic source = findByName(topics, SOURCE_TOPIC_NAME);
    Topic viewTopic = findByName(topics, VIEW_TOPIC_NAME);
    Topic regular = findByName(topics, REGULAR_TOPIC_NAME);

    assertEquals(ImmutableList.of(VIEW_OF_SOURCE), source.getViews());
    // The view topic itself isn't a source, so it has no nested views.
    assertEquals(ImmutableList.of(), viewTopic.getViews());
    // Unrelated regular topic has no views.
    assertEquals(ImmutableList.of(), regular.getViews());
  }

  @Test
  public void view_returnsOnlyViewTopics() {
    List<Topic> topics =
        TopicManagerImpl.mergeViewsAndFilter(THREE_TOPICS, ONE_VIEW_LOOKUP, TopicType.VIEW);

    assertEquals(1, topics.size());
    assertEquals(VIEW_TOPIC_NAME, topics.get(0).getName());
  }

  @Test
  public void standard_excludesViewTopicsButKeepsSources() {
    List<Topic> topics =
        TopicManagerImpl.mergeViewsAndFilter(THREE_TOPICS, ONE_VIEW_LOOKUP, TopicType.STANDARD);

    // The view topic is filtered out; the source and the unrelated regular topic remain.
    assertEquals(2, topics.size());
    assertFalse(topics.stream().anyMatch(t -> t.getName().equals(VIEW_TOPIC_NAME)));
    Topic source = findByName(topics, SOURCE_TOPIC_NAME);
    assertEquals(ImmutableList.of(VIEW_OF_SOURCE), source.getViews());
  }

  @Test
  public void view_withEmptyLookup_returnsNothing() {
    List<Topic> topics =
        TopicManagerImpl.mergeViewsAndFilter(THREE_TOPICS, ImmutableMap.of(), TopicType.VIEW);

    // No topic is classified as a view → VIEW filter excludes everything.
    assertEquals(0, topics.size());
  }

  @Test
  public void all_withEmptyLookup_returnsAllTopicsWithEmptyViews() {
    List<Topic> topics =
        TopicManagerImpl.mergeViewsAndFilter(THREE_TOPICS, ImmutableMap.of(), TopicType.ALL);

    assertEquals(3, topics.size());
    topics.forEach(t -> assertEquals(ImmutableList.of(), t.getViews()));
  }

  private static Topic findByName(List<Topic> topics, String name) {
    return topics.stream().filter(t -> t.getName().equals(name)).findFirst().orElseThrow();
  }
}
