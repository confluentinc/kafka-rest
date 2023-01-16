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

import static io.confluent.kafkarest.common.KafkaFutures.failedFuture;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.exceptions.UnsupportedProtocolException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClusterManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Node NODE_1 = new Node(1, "broker-1", 9091);
  private static final Node NODE_2 = new Node(2, "broker-2", 9092);
  private static final Node NODE_3 = new Node(3, "broker-3", 9093);
  private static final List<Node> NODES = Arrays.asList(NODE_1, NODE_2, NODE_3);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private DescribeClusterResult describeClusterResult;

  private ClusterManagerImpl clusterManager;

  @Before
  public void setUp() {
    clusterManager = new ClusterManagerImpl(adminClient);
  }

  @Test
  public void listClusters_returnListWithOwnCluster() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    List<Cluster> clusters = clusterManager.listClusters().get();

    List<Cluster> expected =
        singletonList(
            Cluster.create(
                CLUSTER_ID,
                Broker.create(CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
                Arrays.asList(
                    Broker.create(
                        CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
                    Broker.create(
                        CLUSTER_ID, NODE_2.id(), NODE_2.host(), NODE_2.port(), NODE_2.rack()),
                    Broker.create(
                        CLUSTER_ID, NODE_3.id(), NODE_3.host(), NODE_3.port(), NODE_3.rack()))));

    assertEquals(expected, clusters);
  }

  @Test
  public void listClusters_noClusterId_throwsUnsupportedProtocol() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(null));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    try {
      clusterManager.listClusters().get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnsupportedProtocolException.class,  e.getCause().getClass());
    }
  }

  @Test
  public void listClusters_noController_returnListWithOwnClusterWithoutController()
      throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(null));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    List<Cluster> clusters = clusterManager.listClusters().get();

    List<Cluster> expected =
        singletonList(
            Cluster.create(
                CLUSTER_ID,
                /* controller= */ null,
                Arrays.asList(
                    Broker.create(
                        CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
                    Broker.create(
                        CLUSTER_ID, NODE_2.id(), NODE_2.host(), NODE_2.port(), NODE_2.rack()),
                    Broker.create(
                        CLUSTER_ID, NODE_3.id(), NODE_3.host(), NODE_3.port(), NODE_3.rack()))));

    assertEquals(expected, clusters);
  }

  @Test
  public void listClusters_noNodes_returnListWithOwnClusterWithoutControllerAndEmptyNodes()
      throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(null));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(null));
    replay(adminClient, describeClusterResult);

    List<Cluster> clusters = clusterManager.listClusters().get();

    List<Cluster> expected =
        singletonList(Cluster.create(CLUSTER_ID, /* controller= */ null, emptyList()));

    assertEquals(expected, clusters);
  }

  @Test
  public void listClusters_timeoutException_returnTimeoutException()
      throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(failedFuture(new TimeoutException()));
    expect(describeClusterResult.controller()).andReturn(failedFuture(new TimeoutException()));
    expect(describeClusterResult.nodes()).andReturn(failedFuture(new TimeoutException()));
    replay(adminClient, describeClusterResult);

    CompletableFuture<List<Cluster>> clusters = clusterManager.listClusters();

    try {
      clusters.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getCluster_ownClusterId_returnsOwnCluster() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    Cluster cluster = clusterManager.getCluster(CLUSTER_ID).get().get();

    Cluster expected =
        Cluster.create(
            CLUSTER_ID,
            Broker.create(CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
            Arrays.asList(
                Broker.create(CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
                Broker.create(CLUSTER_ID, NODE_2.id(), NODE_2.host(), NODE_2.port(), NODE_2.rack()),
                Broker.create(
                    CLUSTER_ID, NODE_3.id(), NODE_3.host(), NODE_3.port(), NODE_3.rack())));

    assertEquals(expected, cluster);
  }

  @Test
  public void getCluster_otherClusterId_returnsEmpty() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    Optional<Cluster> cluster = clusterManager.getCluster("foobar").get();

    assertFalse(cluster.isPresent());
  }

  @Test
  public void getLocalCluster_returnsCluster() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    Cluster cluster = clusterManager.getLocalCluster().get();

    Cluster expected =
        Cluster.create(
            CLUSTER_ID,
            Broker.create(CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
            Arrays.asList(
                Broker.create(CLUSTER_ID, NODE_1.id(), NODE_1.host(), NODE_1.port(), NODE_1.rack()),
                Broker.create(CLUSTER_ID, NODE_2.id(), NODE_2.host(), NODE_2.port(), NODE_2.rack()),
                Broker.create(
                    CLUSTER_ID, NODE_3.id(), NODE_3.host(), NODE_3.port(), NODE_3.rack())));

    assertEquals(expected, cluster);
  }

  @Test
  public void getLocalCluster_noClusterId_throwsUnsupportedProtocol() throws Exception {
    expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(null));
    expect(describeClusterResult.controller()).andReturn(KafkaFuture.completedFuture(NODE_1));
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(NODES));
    replay(adminClient, describeClusterResult);

    try {
      clusterManager.getLocalCluster().get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnsupportedProtocolException.class,  e.getCause().getClass());
    }
  }
}
