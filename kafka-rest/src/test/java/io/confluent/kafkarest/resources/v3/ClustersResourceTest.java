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

package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.singletonList;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ClusterManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetClusterResponse;
import io.confluent.kafkarest.entities.v3.ListClustersResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.common.errors.TimeoutException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClustersResourceTest {

  private static final Broker BROKER_1 = new Broker("cluster-1", 1, "broker-1", 9091, "rack-1");
  private static final Broker BROKER_2 = new Broker("cluster-1", 2, "broker-2", 9092, null);
  private static final Broker BROKER_3 = new Broker("cluster-1", 3, "broker-3", 9093, null);
  private static final Cluster CLUSTER_1 =
      new Cluster("cluster-1", BROKER_1, Arrays.asList(BROKER_1, BROKER_2, BROKER_3));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ClusterManager clusterManager;

  private ClustersResource clustersResource;

  @Before
  public void setUp() {
    clustersResource =
        new ClustersResource(
            () -> clusterManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""), new FakeUrlFactory());
  }

  @Test
  public void listClusters_returnsArrayWithOwnClusters() {
    expect(clusterManager.listClusters())
        .andReturn(CompletableFuture.completedFuture(singletonList(CLUSTER_1)));
    replay(clusterManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clustersResource.listClusters(response);

    ListClustersResponse expected =
        new ListClustersResponse(
            new CollectionLink("/v3/clusters", /* next= */ null),
            singletonList(
                new ClusterData(
                    "crn:///kafka=cluster-1",
                    new ResourceLink("/v3/clusters/cluster-1"),
                    "cluster-1",
                    new Relationship("/v3/clusters/cluster-1/brokers/1"),
                    new Relationship("/v3/clusters/cluster-1/brokers"),
                    new Relationship("/v3/clusters/cluster-1/topics"))));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listClusters_timeoutException_returnsTimeoutException() {
    expect(clusterManager.listClusters()).andReturn(failedFuture(new TimeoutException()));
    replay(clusterManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clustersResource.listClusters(response);

    assertEquals(TimeoutException.class, response.getException().getClass());
  }

  @Test
  public void getCluster_ownCluster_returnsCluster() {
    expect(clusterManager.getCluster(CLUSTER_1.getClusterId()))
        .andReturn(CompletableFuture.completedFuture(Optional.of(CLUSTER_1)));
    replay(clusterManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clustersResource.getCluster(response, CLUSTER_1.getClusterId());

    GetClusterResponse expected =
        new GetClusterResponse(
            new ClusterData(
                "crn:///kafka=cluster-1",
                new ResourceLink("/v3/clusters/cluster-1"),
                "cluster-1",
                new Relationship("/v3/clusters/cluster-1/brokers/1"),
                new Relationship("/v3/clusters/cluster-1/brokers"),
                new Relationship("/v3/clusters/cluster-1/topics")));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getCluster_otherCluster_returnsNotFoundException() {
    expect(clusterManager.getCluster("foobar"))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(clusterManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clustersResource.getCluster(response, "foobar");

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getCluster_timeoutException_returnsTimeoutException() {
    expect(clusterManager.getCluster(CLUSTER_1.getClusterId()))
        .andReturn(failedFuture(new TimeoutException()));
    replay(clusterManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clustersResource.getCluster(response, CLUSTER_1.getClusterId());

    assertEquals(TimeoutException.class, response.getException().getClass());
  }
}
