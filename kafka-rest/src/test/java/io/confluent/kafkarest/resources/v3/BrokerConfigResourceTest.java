package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.controllers.BrokerConfigManager;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.UpdateBrokerConfigRequest;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BrokerConfigResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final int BROKER_ID = 1;

  private static final BrokerConfig CONFIG_1 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive */ false,
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
          /* isSensitive */ false,
          ConfigSource.STATIC_BROKER_CONFIG,
          /* synonyms= */ emptyList());
  private static final BrokerConfig CONFIG_3 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-3",
          "value-3",
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive */ true,
          ConfigSource.DYNAMIC_BROKER_CONFIG,
          /* synonyms= */ emptyList());

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private BrokerConfigManager brokerConfigManager;

  private BrokerConfigsResource brokerConfigsResource;

  @Before
  public void setUp() {
    brokerConfigsResource =
        new BrokerConfigsResource(
            () -> brokerConfigManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */""),
            new FakeUrlFactory());
  }

  @Test
  public void listBrokerConfigs_existingBroker_returnsConfigs() {
    expect(brokerConfigManager.listBrokerConfigs(CLUSTER_ID, BROKER_ID))
        .andReturn(
            completedFuture(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.listBrokerConfigs(response, CLUSTER_ID, BROKER_ID);

    ListBrokerConfigsResponse expected =
        new ListBrokerConfigsResponse(
            new CollectionLink(
                "/v3/clusters/cluster-1/brokers/1/configs", null),
            Arrays.asList(
                new BrokerConfigData(
                    "crn:///kafka=cluster-1/broker=1/config=config-1",
                    new ResourceLink("/v3/clusters/cluster-1/brokers/1/configs/config-1"),
                    CLUSTER_ID,
                    BROKER_ID,
                    CONFIG_1.getName(),
                    CONFIG_1.getValue(),
                    CONFIG_1.isDefault(),
                    CONFIG_1.isReadOnly(),
                    CONFIG_1.isSensitive(),
                    CONFIG_1.getSource(),
                    CONFIG_1.getSynonyms().stream()
                        .map(ConfigSynonymData::fromConfigSynonym)
                        .collect(Collectors.toList())),
                new BrokerConfigData(
                    "crn:///kafka=cluster-1/broker=1/config=config-2",
                    new ResourceLink("/v3/clusters/cluster-1/brokers/1/configs/config-2"),
                    CLUSTER_ID,
                    BROKER_ID,
                    CONFIG_2.getName(),
                    CONFIG_2.getValue(),
                    CONFIG_2.isDefault(),
                    CONFIG_2.isReadOnly(),
                    CONFIG_2.isSensitive(),
                    CONFIG_2.getSource(),
                    CONFIG_2.getSynonyms().stream()
                        .map(ConfigSynonymData::fromConfigSynonym)
                        .collect(Collectors.toList())),
                new BrokerConfigData(
                    "crn:///kafka=cluster-1/broker=1/config=config-3",
                    new ResourceLink("/v3/clusters/cluster-1/brokers/1/configs/config-3"),
                    CLUSTER_ID,
                    BROKER_ID,
                    CONFIG_3.getName(),
                    CONFIG_3.getValue(),
                    CONFIG_3.isDefault(),
                    CONFIG_3.isReadOnly(),
                    CONFIG_3.isSensitive(),
                    CONFIG_3.getSource(),
                    CONFIG_3.getSynonyms().stream()
                        .map(ConfigSynonymData::fromConfigSynonym)
                        .collect(Collectors.toList()))));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listBrokerConfigs_nonExistingBrokerOrCluster_throwsNotFound() {
    expect(brokerConfigManager.listBrokerConfigs(CLUSTER_ID, BROKER_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.listBrokerConfigs(response, CLUSTER_ID, BROKER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getBrokerConfig_existingConfig_returnsConfig() {
    expect(brokerConfigManager.getBrokerConfig(
        CLUSTER_ID, BROKER_ID, CONFIG_1.getName()))
        .andReturn(completedFuture(Optional.of(CONFIG_1)));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.getBrokerConfig(response, CLUSTER_ID, BROKER_ID, CONFIG_1.getName());
    GetBrokerConfigResponse expected =
        new GetBrokerConfigResponse(
            new BrokerConfigData(
                "crn:///kafka=cluster-1/broker=1/config=config-1",
                new ResourceLink("/v3/clusters/cluster-1/brokers/1/configs/config-1"),
                CLUSTER_ID,
                BROKER_ID,
                CONFIG_1.getName(),
                CONFIG_1.getValue(),
                CONFIG_1.isDefault(),
                CONFIG_1.isReadOnly(),
                CONFIG_1.isSensitive(),
                CONFIG_1.getSource(),
                CONFIG_1.getSynonyms().stream()
                    .map(ConfigSynonymData::fromConfigSynonym)
                    .collect(Collectors.toList())));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getBrokerConfig_nonExistingConfig_throwsNotFound() {
    expect(brokerConfigManager.getBrokerConfig(
        CLUSTER_ID, BROKER_ID, CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.getBrokerConfig(
        response, CLUSTER_ID, BROKER_ID, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getBrokerConfig_nonExistingBrokerorCluster_throwsNotFound() {
    expect(brokerConfigManager.getBrokerConfig(
        CLUSTER_ID, BROKER_ID, CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.getBrokerConfig(
        response, CLUSTER_ID, BROKER_ID, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void updateBrokerConfig_existingConfig_updatesConfig() {
    expect(
        brokerConfigManager.updateBrokerConfig(
            CLUSTER_ID,
            BROKER_ID,
            CONFIG_1.getName(),
            "new-value"))
        .andReturn(completedFuture(null));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.updateBrokerConfig(
        response,
        CLUSTER_ID,
        BROKER_ID,
        CONFIG_1.getName(),
        new UpdateBrokerConfigRequest(
            new UpdateBrokerConfigRequest.Data(
                new UpdateBrokerConfigRequest.Data.Attributes("new-value"))));
    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void updateConfig_nonExistingConfigOrBrokerOrCluster_throwsNotFound() {
    expect(
        brokerConfigManager.updateBrokerConfig(
            CLUSTER_ID,
            BROKER_ID,
            CONFIG_1.getName(),
            "new-value"))
        .andReturn(failedFuture(new NotFoundException()));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.updateBrokerConfig(
        response,
        CLUSTER_ID,
        BROKER_ID,
        CONFIG_1.getName(),
        new UpdateBrokerConfigRequest(
            new UpdateBrokerConfigRequest.Data(
                new UpdateBrokerConfigRequest.Data.Attributes("new-value"))));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void resetBrokerConfig_existingConfig_resetsConfig() {
    expect(
        brokerConfigManager.resetBrokerConfig(
            CLUSTER_ID,
            BROKER_ID,
            CONFIG_1.getName()))
        .andReturn(completedFuture(null));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.resetBrokerConfig(
        response, CLUSTER_ID, BROKER_ID, CONFIG_1.getName());

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void resetBrokerConfig_nonExistingConfigOrBrokerOrCluster_throwsNotFound() {
    expect(
        brokerConfigManager.resetBrokerConfig(
            CLUSTER_ID,
            BROKER_ID,
            CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    brokerConfigsResource.resetBrokerConfig(
        response, CLUSTER_ID, BROKER_ID, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
