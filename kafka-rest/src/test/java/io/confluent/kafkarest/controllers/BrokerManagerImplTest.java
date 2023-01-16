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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BrokerManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Broker BROKER_1 = Broker.create(CLUSTER_ID, 1, "1.0.0.1", 1, null);
  private static final Broker BROKER_2 = Broker.create(CLUSTER_ID, 2, "1.0.0.2", 2, null);
  private static final Broker BROKER_3 = Broker.create(CLUSTER_ID, 2, "1.0.0.3", 3, "rack-3");

  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, BROKER_1, Arrays.asList(BROKER_1, BROKER_2, BROKER_3));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ClusterManager clusterManager;

  private BrokerManagerImpl brokerManager;

  @Before
  public void setUp() {
    brokerManager = new BrokerManagerImpl(clusterManager);
  }

  @Test
  public void listBrokers_existingCluster_returnsBrokers() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(CompletableFuture.completedFuture(Optional.of(CLUSTER)));
    replay(clusterManager);

    List<Broker> brokers = brokerManager.listBrokers(CLUSTER_ID).get();

    assertEquals(CLUSTER.getBrokers(), brokers);
  }

  @Test
  public void listBrokers_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      brokerManager.listBrokers(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getBroker_existingClusterExistingBroker_returnsBroker() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(CompletableFuture.completedFuture(Optional.of(CLUSTER)));
    replay(clusterManager);

    Optional<Broker> broker = brokerManager.getBroker(CLUSTER_ID, 1).get();

    assertEquals(BROKER_1, broker.get());
  }

  @Test
  public void getBroker_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      brokerManager.getBroker(CLUSTER_ID, 1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getBroker_nonExistingBroker_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(CompletableFuture.completedFuture(Optional.of(CLUSTER)));
    replay(clusterManager);

    Optional<Broker> broker = brokerManager.getBroker(CLUSTER_ID, 4).get();

    assertFalse(broker.isPresent());
  }

  @Test
  public void listLocalBrokers_returnsBrokers() throws Exception {
    expect(clusterManager.getLocalCluster()).andReturn(CompletableFuture.completedFuture(CLUSTER));
    replay(clusterManager);

    List<Broker> brokers = brokerManager.listLocalBrokers().get();

    assertEquals(CLUSTER.getBrokers(), brokers);
  }
}
