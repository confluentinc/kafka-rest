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

package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Test;

public class ListAllBrokersConfigsActionIntegrationTest extends ClusterTestHarness {

  public ListAllBrokersConfigsActionIntegrationTest() {
    super(/* numBrokers= */ 2, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokersConfigs_existingBrokers_returnsConfigs() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ResourceCollection.Metadata expectedMetadata =
        ResourceCollection.Metadata.builder()
            .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/brokers/-/configs")
            .build();

    BrokerConfigData expectedBroker1Config1 =
        createBrokerConfigData(
            baseUrl,
            clusterId,
            0,
            "log.cleaner.min.compaction.lag.ms",
            "0",
            true,
            false,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("log.cleaner.min.compaction.lag.ms")
                    .setValue("0")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    BrokerConfigData expectedBroker1Config2 =
        createBrokerConfigData(
            baseUrl,
            clusterId,
            0,
            "offsets.topic.num.partitions",
            "5",
            false,
            true,
            ConfigSource.STATIC_BROKER_CONFIG,
            ImmutableList.of(
                ConfigSynonymData.builder()
                    .setName("offsets.topic.num.partitions")
                    .setValue("5")
                    .setSource(ConfigSource.STATIC_BROKER_CONFIG)
                    .build(),
                ConfigSynonymData.builder()
                    .setName("offsets.topic.num.partitions")
                    .setValue("50")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    BrokerConfigData expectedBroker2Config1 =
        createBrokerConfigData(
            baseUrl,
            clusterId,
            1,
            "num.network.threads",
            "3",
            true,
            false,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("num.network.threads")
                    .setValue("3")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    BrokerConfigData expectedBroker2Config2 =
        createBrokerConfigData(
            baseUrl,
            clusterId,
            1,
            "default.replication.factor",
            "1",
            false,
            true,
            ConfigSource.STATIC_BROKER_CONFIG,
            ImmutableList.of(
                ConfigSynonymData.builder()
                    .setName("default.replication.factor")
                    .setValue("1")
                    .setSource(ConfigSource.STATIC_BROKER_CONFIG)
                    .build(),
                ConfigSynonymData.builder()
                    .setName("default.replication.factor")
                    .setValue("1")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    BrokerConfigData expectedBroker2Config3 =
        createBrokerConfigData(
            baseUrl,
            clusterId,
            1,
            "queued.max.request.bytes",
            "-1",
            true,
            true,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("queued.max.request.bytes")
                    .setValue("-1")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/-/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ListBrokerConfigsResponse responseBody = response.readEntity(ListBrokerConfigsResponse.class);
    assertEquals(expectedMetadata, responseBody.getValue().getMetadata());

    assertEquals(
        2,
        responseBody.getValue().getData().stream()
            .collect(Collectors.toMap(BrokerConfigData::getBrokerId, config -> config, (o, n) -> n))
            .size(),
        "Unexpected number of brokers in response");
    assertTrue(
        responseBody.getValue().getData().contains(expectedBroker1Config1),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedBroker1Config1));
    assertTrue(
        responseBody.getValue().getData().contains(expectedBroker1Config2),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedBroker1Config2));
    assertTrue(
        responseBody.getValue().getData().contains(expectedBroker2Config1),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedBroker2Config1));
    assertTrue(
        responseBody.getValue().getData().contains(expectedBroker2Config2),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedBroker2Config2));
    assertTrue(
        responseBody.getValue().getData().contains(expectedBroker2Config3),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedBroker2Config3));
  }

  @Test
  public void listBrokerConfigs_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/brokers/-/configs").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  private BrokerConfigData createBrokerConfigData(
      String baseUrl,
      String clusterId,
      Integer brokerId,
      String configName,
      String configValue,
      boolean isDefault,
      boolean isReadonly,
      ConfigSource source,
      List<ConfigSynonymData> synonyms) {
    return BrokerConfigData.builder()
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    baseUrl
                        + "/v3/clusters/"
                        + clusterId
                        + "/brokers/"
                        + brokerId
                        + "/configs/"
                        + configName)
                .setResourceName(
                    "crn:///kafka=" + clusterId + "/broker=" + brokerId + "/config=" + configName)
                .build())
        .setClusterId(clusterId)
        .setBrokerId(brokerId)
        .setName(configName)
        .setValue(configValue)
        .setDefault(isDefault)
        .setReadOnly(isReadonly)
        .setSensitive(false)
        .setSource(source)
        .setSynonyms(synonyms)
        .build();
  }
}
