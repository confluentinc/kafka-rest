/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafkarest.v2;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.resources.v2.PartitionsResource;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.util.Arrays;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.easymock.Mock;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;

public class PartitionsResourceTest extends JerseyTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final Partition PARTITION =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 0,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 1,
                  /* isLeader= */ true,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ false)),
          /* earliestOffset= */ 100L,
          /* latestOffset= */ 1000L);

  @Mock
  private PartitionManager partitionManager;

  @Override
  protected Application configure() {
    partitionManager = createMock(PartitionManager.class);
    ResourceConfig application = new ResourceConfig();
    application.register(new PartitionsResource(() -> partitionManager));
    application.register(new JacksonMessageBodyProvider());
    return application;
  }

  @Before
  public void setUpMocks() {
    reset(partitionManager);
  }

  @Test
  public void getOffsets_returnsBeginningAndEnd() {
    expect(partitionManager.getLocalPartition(TOPIC_NAME, PARTITION.getPartitionId()))
        .andReturn(completedFuture(Optional.of(PARTITION)));
    replay(partitionManager);

    String response =
        target("/topics/{topic}/partitions/{partition}/offsets")
            .resolveTemplate("topic", TOPIC_NAME)
            .resolveTemplate("partition", PARTITION.getPartitionId())
            .request()
            .get(String.class);

    assertEquals(
        String.format(
            "{\"beginning_offset\":%d,\"end_offset\":%d}",
            PARTITION.getEarliestOffset(),
            PARTITION.getLatestOffset()),
        response);

    verify(partitionManager);
  }

  @Test
  public void getOffsets_topicDoesNotExist_returns404() {
    expect(partitionManager.getLocalPartition(TOPIC_NAME, PARTITION.getPartitionId()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    Response response =
        target("/topics/{topic}/partitions/{partition}/offsets")
            .resolveTemplate("topic", TOPIC_NAME)
            .resolveTemplate("partition", PARTITION.getPartitionId())
            .request()
            .get();

    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    verify(partitionManager);
  }

  @Test
  public void getOffsets_partitionDoesNotExist_returns404() {
    expect(partitionManager.getLocalPartition(TOPIC_NAME, PARTITION.getPartitionId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(partitionManager);

    Response response =
        target("/topics/{topic}/partitions/{partition}/offsets")
            .resolveTemplate("topic", TOPIC_NAME)
            .resolveTemplate("partition", PARTITION.getPartitionId())
            .request()
            .get();

    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    verify(partitionManager);
  }
}
