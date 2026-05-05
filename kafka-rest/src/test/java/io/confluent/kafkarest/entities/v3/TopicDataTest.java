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

package io.confluent.kafkarest.entities.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TopicDataTest {

  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() {
    mapper = new ObjectMapper();
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new Jdk8Module());
  }

  /** Builds a minimal TopicData with all required fields set. {@code views} is left at default. */
  private static TopicData.Builder baseTopicDataBuilder() {
    return TopicData.builder()
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf("https://example/v3/clusters/c1/topics/t1")
                .setResourceName("crn:///kafka=c1/topic=t1")
                .build())
        .setClusterId("c1")
        .setTopicName("t1")
        .setInternal(false)
        .setReplicationFactor(3)
        .setPartitionsCount(6)
        .setPartitions(Resource.Relationship.create("https://example/.../partitions"))
        .setConfigs(Resource.Relationship.create("https://example/.../configs"))
        .setPartitionReassignments(
            Resource.Relationship.create("https://example/.../-/reassignment"))
        .setAuthorizedOperations(ImmutableSet.of());
  }

  private static TopicViewInfo sampleView() {
    return TopicViewInfo.builder()
        .setViewTopicName("view")
        .setViewTopicId("viewTopicId")
        .setViewFilter("SELECT * FROM source")
        .setFlinkStatementId("statementId")
        .setFlinkComputePoolId("poolId")
        .setViewStatus("ACTIVE")
        .setViewStatusMessage(null)
        .setStatusChangedAt(1714780823000L)
        .build();
  }

  @Test
  public void serialize_omitsViewsFieldWhenEmpty() throws JsonProcessingException {
    TopicData data = baseTopicDataBuilder().build();

    String json = mapper.writeValueAsString(data);

    assertFalse(
        json.contains("\"views\""),
        "views should be omitted from JSON when empty, but got: " + json);
  }

  @Test
  public void serialize_includesViewsArrayWhenPopulated() throws JsonProcessingException {
    TopicData data = baseTopicDataBuilder().setViews(ImmutableList.of(sampleView())).build();

    String json = mapper.writeValueAsString(data);

    assertTrue(json.contains("\"views\""), "views field should appear in JSON");
    assertTrue(json.contains("\"view_topic_name\":\"view\""));
    assertTrue(json.contains("\"view_topic_id\":\"viewTopicId\""));
    assertTrue(json.contains("\"view_filter\":\"SELECT * FROM source\""));
    assertTrue(json.contains("\"flink_statement_id\":\"statementId\""));
    assertTrue(json.contains("\"flink_compute_pool_id\":\"poolId\""));
    assertTrue(json.contains("\"view_status\":\"ACTIVE\""));
    assertTrue(json.contains("\"view_status_message\":null"));
    assertTrue(json.contains("\"status_changed_at\":1714780823000"));
  }

  @Test
  public void deserialize_missingViewsFieldDefaultsToEmptyList() throws JsonProcessingException {
    String jsonWithoutViews =
        "{\"kind\":\"KafkaTopic\","
            + "\"metadata\":{\"self\":\"https://example/v3/clusters/c1/topics/t1\","
            + "\"resource_name\":\"crn:///kafka=c1/topic=t1\"},"
            + "\"cluster_id\":\"c1\",\"topic_name\":\"t1\",\"is_internal\":false,"
            + "\"replication_factor\":3,\"partitions_count\":6,"
            + "\"partitions\":{\"related\":\"https://example/.../partitions\"},"
            + "\"configs\":{\"related\":\"https://example/.../configs\"},"
            + "\"partition_reassignments\":{\"related\":\"https://example/.../-/reassignment\"},"
            + "\"authorized_operations\":[]}";

    TopicData data = mapper.readValue(jsonWithoutViews, TopicData.class);

    assertEquals(ImmutableList.of(), data.getViews());
  }

  @Test
  public void deserialize_populatedViewsFieldRoundTrips() throws JsonProcessingException {
    TopicData original = baseTopicDataBuilder().setViews(ImmutableList.of(sampleView())).build();

    String json = mapper.writeValueAsString(original);
    TopicData reparsed = mapper.readValue(json, TopicData.class);

    assertEquals(original.getViews(), reparsed.getViews());
    assertEquals(1, reparsed.getViews().size());
    TopicViewInfo view = reparsed.getViews().get(0);
    assertEquals("view", view.getViewTopicName());
    assertEquals("viewTopicId", view.getViewTopicId());
    assertEquals("ACTIVE", view.getViewStatus());
    assertEquals(1714780823000L, view.getStatusChangedAt());
  }
}
