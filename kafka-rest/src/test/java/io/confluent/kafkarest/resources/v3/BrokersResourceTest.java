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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.BrokerDataList;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Metadata;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BrokersResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Broker BROKER_1 = Broker.create(CLUSTER_ID, 1, "broker-1", 9091, "rack-1");
  private static final Broker BROKER_2 = Broker.create(CLUSTER_ID, 2, "broker-2", 9092, null);
  private static final Broker BROKER_3 = Broker.create(CLUSTER_ID, 3, "broker-3", 9093, null);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private BrokerManager brokerManager;

  private BrokersResource brokersResource;

  @Before
  public void setUp() {
    brokersResource =
        new BrokersResource(
            () -> brokerManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listBrokers_existingCluster_returnsBrokers() {
    expect(brokerManager.listBrokers(CLUSTER_ID))
        .andReturn(CompletableFuture.completedFuture(Arrays.asList(BROKER_1, BROKER_2, BROKER_3)));
    replay(brokerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokersResource.listBrokers(response, CLUSTER_ID);

    ListBrokersResponse expected =
        ListBrokersResponse.create(
            BrokerDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/brokers")
                        .build())
                .setData(
                    Arrays.asList(
                        BrokerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/brokers/1")
                                    .setResourceName("crn:///kafka=cluster-1/broker=1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setBrokerId(BROKER_1.getBrokerId())
                            .setHost(BROKER_1.getHost())
                            .setPort(BROKER_1.getPort())
                            .setRack(BROKER_1.getRack())
                            .setConfigs(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/brokers/1/configs"))
                            .setPartitionReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/brokers/1/partition-replicas"))
                            .build(),
                        BrokerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/brokers/2")
                                    .setResourceName("crn:///kafka=cluster-1/broker=2")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setBrokerId(BROKER_2.getBrokerId())
                            .setHost(BROKER_2.getHost())
                            .setPort(BROKER_2.getPort())
                            .setRack(BROKER_2.getRack())
                            .setConfigs(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/brokers/2/configs"))
                            .setPartitionReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/brokers/2/partition-replicas"))
                            .build(),
                        BrokerData.builder()
                            .setMetadata(
                                Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/brokers/3")
                                    .setResourceName("crn:///kafka=cluster-1/broker=3")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setBrokerId(BROKER_3.getBrokerId())
                            .setHost(BROKER_3.getHost())
                            .setPort(BROKER_3.getPort())
                            .setRack(BROKER_3.getRack())
                            .setConfigs(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/brokers/3/configs"))
                            .setPartitionReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/brokers/3/partition-replicas"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listBrokers_nonExistingCluster_throwsNotFound() {
    expect(brokerManager.listBrokers(CLUSTER_ID)).andReturn(failedFuture(new NotFoundException()));
    replay(brokerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokersResource.listBrokers(response, CLUSTER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getBroker_existingClusterExistingBroker_returnsBroker() {
    expect(brokerManager.getBroker(CLUSTER_ID, BROKER_1.getBrokerId()))
        .andReturn(CompletableFuture.completedFuture(Optional.of(BROKER_1)));
    replay(brokerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokersResource.getBroker(response, CLUSTER_ID, BROKER_1.getBrokerId());

    GetBrokerResponse expected =
        GetBrokerResponse.create(
            BrokerData.builder()
                .setMetadata(
                    Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/brokers/1")
                        .setResourceName("crn:///kafka=cluster-1/broker=1")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setBrokerId(BROKER_1.getBrokerId())
                .setHost(BROKER_1.getHost())
                .setPort(BROKER_1.getPort())
                .setRack(BROKER_1.getRack())
                .setConfigs(
                    Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1/configs"))
                .setPartitionReplicas(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/brokers/1/partition-replicas"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getBroker_nonExistingCluster_throwsNotFound() {
    expect(brokerManager.getBroker(CLUSTER_ID, 1)).andReturn(failedFuture(new NotFoundException()));
    replay(brokerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokersResource.getBroker(response, CLUSTER_ID, 1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getBroker_existingClusterNonExistingBroker_throwsNotFound() {
    expect(brokerManager.getBroker(CLUSTER_ID, 4))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(brokerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokersResource.getBroker(response, CLUSTER_ID, 4);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
