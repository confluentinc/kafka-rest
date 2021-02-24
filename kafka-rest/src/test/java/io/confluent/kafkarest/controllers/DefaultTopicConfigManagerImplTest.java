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

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.TopicConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class DefaultTopicConfigManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";
  private static final int BROKER_ID = 1;
  private static final short DEFAULT_REPLICATION_FACTOR = 1;
  private static final int DEFAULT_PARTITIONS = 2;

  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, /* controller= */ null, emptyList());

  private static final TopicConfig CONFIG_1 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DEFAULT_CONFIG,
          /* synonyms= */ emptyList());
  private static final TopicConfig CONFIG_2 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive= */ false,
          ConfigSource.UNKNOWN,
          /* synonyms= */ emptyList());
  private static final TopicConfig CONFIG_3 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-3",
          null,
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true,
          ConfigSource.UNKNOWN,
          /* synonyms= */ emptyList());

  private static final Config CONFIG =
      new Config(
          Arrays.asList(
              new ConfigEntry(
                  CONFIG_1.getName(),
                  CONFIG_1.getValue(),
                  CONFIG_1.isDefault(),
                  CONFIG_1.isSensitive(),
                  CONFIG_1.isReadOnly()),
              new ConfigEntry(
                  CONFIG_2.getName(),
                  CONFIG_2.getValue(),
                  CONFIG_2.isDefault(),
                  CONFIG_2.isSensitive(),
                  CONFIG_2.isReadOnly()),
              new ConfigEntry(
                  CONFIG_3.getName(),
                  CONFIG_3.getValue(),
                  CONFIG_3.isDefault(),
                  CONFIG_3.isSensitive(),
                  CONFIG_3.isReadOnly())));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private ClusterManager clusterManager;

  @Mock
  private BrokerManager brokerManager;

  @Mock
  private BrokerConfigManager brokerConfigManager;

  @Mock
  private CreateTopicsResult createTopicsResult;

  @Mock
  private AlterConfigsResult alterConfigsResult;

  private DefaultTopicConfigManagerImpl defaultTopicConfigManager;

  @Before
  public void setUp() {
    defaultTopicConfigManager = new DefaultTopicConfigManagerImpl(adminClient, clusterManager,
        brokerConfigManager, brokerManager);
  }

  @Test
  public void listTopicConfigs_nonExistingTopic_returnsConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.createTopics(
            eq(singletonList(new NewTopic(TOPIC_NAME,Optional.empty(),Optional.empty()))),
            anyObject(CreateTopicsOptions.class)))
        .andReturn(createTopicsResult);
    expect(createTopicsResult.config(TOPIC_NAME))
        .andReturn(
            KafkaFuture.completedFuture(CONFIG));
    replay(adminClient, clusterManager, createTopicsResult);

    List<TopicConfig> configs = defaultTopicConfigManager.listDefaultTopicConfigs(
        CLUSTER_ID, TOPIC_NAME).get();

    assertEquals(
        new HashSet<>(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)), new HashSet<>(configs));
  }

  @Test
  public void listTopicConfigs_oldBroker_returnsConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        brokerManager.listBrokers(CLUSTER_ID)
    ).andReturn(CompletableFuture.completedFuture(
        Arrays.asList(
            Broker.create(CLUSTER_ID,BROKER_ID,null,null,null))
    ));
    expect(
        brokerConfigManager.listBrokerConfigs(CLUSTER_ID,BROKER_ID)
    ).andReturn(CompletableFuture.completedFuture(Arrays.asList(
        BrokerConfig.create(CLUSTER_ID,BROKER_ID,
            KafkaConfig.DefaultReplicationFactorProp(),
            String.valueOf(DEFAULT_REPLICATION_FACTOR),
            true,
            false,
            false,
            ConfigSource.DEFAULT_CONFIG,
            Collections.emptyList()),
        BrokerConfig.create(CLUSTER_ID,BROKER_ID,
            KafkaConfig.NumPartitionsProp(),
            String.valueOf(DEFAULT_PARTITIONS),
            true,
            false,
            false,
            ConfigSource.DEFAULT_CONFIG,
            Collections.emptyList())
        )));
    expect(
        adminClient.createTopics(
            eq(singletonList(new NewTopic(TOPIC_NAME,Optional.empty(),Optional.empty()))),
            anyObject(CreateTopicsOptions.class)))
        .andThrow(new UnsupportedVersionException("This broker is too old"));
    expect(
        adminClient.createTopics(
            eq(singletonList(new NewTopic(
                TOPIC_NAME,
                DEFAULT_PARTITIONS,
                DEFAULT_REPLICATION_FACTOR))),
            anyObject(CreateTopicsOptions.class)))
        .andReturn(createTopicsResult);
    expect(createTopicsResult.config(TOPIC_NAME))
        .andReturn(
            KafkaFuture.completedFuture(CONFIG));
    replay(adminClient, clusterManager, brokerConfigManager, brokerManager, createTopicsResult);

    List<TopicConfig> configs = defaultTopicConfigManager.listDefaultTopicConfigs(
        CLUSTER_ID, TOPIC_NAME).get();

    assertEquals(
        new HashSet<>(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)), new HashSet<>(configs));
  }

}
