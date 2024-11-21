/*
 * Copyright 2021 Confluent Inc.
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
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.controllers.BrokerConfigManager;
import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.BrokerConfigDataList;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ListAllBrokersConfigsActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Integer BROKER_ID = 1;

  private static final BrokerConfig CONFIG_1 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DEFAULT_CONFIG,
          /* synonyms= */ emptyList());
  private static final BrokerConfig CONFIG_2 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive= */ false,
          ConfigSource.DYNAMIC_BROKER_CONFIG,
          /* synonyms= */ emptyList());
  private static final BrokerConfig CONFIG_3 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-3",
          null,
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true,
          ConfigSource.DYNAMIC_BROKER_CONFIG,
          /* synonyms= */ emptyList());

  @Mock private BrokerConfigManager brokerConfigManager;

  @Mock private BrokerManager brokerManager;

  private ListAllBrokersConfigsAction allBrokersConfigsAction;

  @BeforeEach
  public void setUp() {
    allBrokersConfigsAction =
        new ListAllBrokersConfigsAction(
            () -> brokerManager,
            () -> brokerConfigManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listAllBrokerConfigs_existingBrokers_returnsConfigs() {
    expect(brokerManager.listBrokers(CLUSTER_ID))
        .andReturn(
            completedFuture(
                Arrays.asList(Broker.create(CLUSTER_ID, BROKER_ID, "localhost", 9092, "us-east"))));

    expect(brokerConfigManager.listAllBrokerConfigs(CLUSTER_ID, Arrays.asList(BROKER_ID)))
        .andReturn(
            completedFuture(
                new HashMap<Integer, List<BrokerConfig>>() {
                  {
                    put(BROKER_ID, Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3));
                  }
                }));
    replay(brokerManager, brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allBrokersConfigsAction.listBrokersConfigs(response, CLUSTER_ID);

    ListBrokerConfigsResponse expected =
        ListBrokerConfigsResponse.create(
            BrokerConfigDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/brokers/-/configs")
                        .build())
                .setData(
                    Arrays.asList(
                        BrokerConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/brokers/1/configs/config-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/broker=1/config=config-1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setBrokerId(BROKER_ID)
                            .setName(CONFIG_1.getName())
                            .setValue(CONFIG_1.getValue())
                            .setDefault(CONFIG_1.isDefault())
                            .setReadOnly(CONFIG_1.isReadOnly())
                            .setSensitive(CONFIG_1.isSensitive())
                            .setSource(CONFIG_1.getSource())
                            .setSynonyms(
                                CONFIG_1.getSynonyms().stream()
                                    .map(ConfigSynonymData::fromConfigSynonym)
                                    .collect(Collectors.toList()))
                            .build(),
                        BrokerConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/brokers/1/configs/config-2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/broker=1/config=config-2")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setBrokerId(BROKER_ID)
                            .setName(CONFIG_2.getName())
                            .setValue(CONFIG_2.getValue())
                            .setDefault(CONFIG_2.isDefault())
                            .setReadOnly(CONFIG_2.isReadOnly())
                            .setSensitive(CONFIG_2.isSensitive())
                            .setSource(CONFIG_2.getSource())
                            .setSynonyms(
                                CONFIG_2.getSynonyms().stream()
                                    .map(ConfigSynonymData::fromConfigSynonym)
                                    .collect(Collectors.toList()))
                            .build(),
                        BrokerConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/brokers/1/configs/config-3")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/broker=1/config=config-3")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setBrokerId(BROKER_ID)
                            .setName(CONFIG_3.getName())
                            .setValue(CONFIG_3.getValue())
                            .setDefault(CONFIG_3.isDefault())
                            .setReadOnly(CONFIG_3.isReadOnly())
                            .setSensitive(CONFIG_3.isSensitive())
                            .setSource(CONFIG_3.getSource())
                            .setSynonyms(
                                CONFIG_3.getSynonyms().stream()
                                    .map(ConfigSynonymData::fromConfigSynonym)
                                    .collect(Collectors.toList()))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listAllBrokerConfigs_noBrokers_returnsEmptyConfigs() {
    expect(brokerManager.listBrokers(CLUSTER_ID)).andReturn(completedFuture(new ArrayList<>()));

    expect(brokerConfigManager.listAllBrokerConfigs(CLUSTER_ID, new ArrayList<>()))
        .andReturn(completedFuture(new HashMap<Integer, List<BrokerConfig>>()));
    replay(brokerManager, brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allBrokersConfigsAction.listBrokersConfigs(response, CLUSTER_ID);

    ListBrokerConfigsResponse expected =
        ListBrokerConfigsResponse.create(
            BrokerConfigDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/brokers/-/configs")
                        .build())
                .setData(new ArrayList<>())
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listAllBrokerConfigs_nonExistingCluster_throwsNotFound() {
    expect(brokerManager.listBrokers(CLUSTER_ID)).andReturn(failedFuture(new NotFoundException()));
    replay(brokerManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allBrokersConfigsAction.listBrokersConfigs(response, CLUSTER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
