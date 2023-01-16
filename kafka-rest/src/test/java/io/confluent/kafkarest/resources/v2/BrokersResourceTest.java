/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.resources.v2;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.v2.BrokerList;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.util.Arrays;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.protocol.Errors;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BrokersResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Broker BROKER_1 = Broker.create(CLUSTER_ID, 1, "host1", 1, /* rack= */ null);
  private static final Broker BROKER_2 = Broker.create(CLUSTER_ID, 2, "host2", 2, /* rack= */ null);
  private static final Broker BROKER_3 = Broker.create(CLUSTER_ID, 3, "host3", 3, /* rack= */ null);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private BrokerManager brokerManager;

  public BrokersResourceTest() throws RestConfigException {
    super();
  }

  @Before
  public void setUp() throws Exception {
    addResource(new BrokersResource(() -> brokerManager));
    super.setUp();
  }

  @Test
  public void testList() {
    expect(brokerManager.listLocalBrokers())
        .andReturn(completedFuture(Arrays.asList(BROKER_1, BROKER_2, BROKER_3)));
    replay(brokerManager);

    Response response = request("/brokers", Versions.KAFKA_V2_JSON).get();
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final BrokerList returnedBrokerIds = TestUtils.tryReadEntityOrLog(response, new GenericType<BrokerList>() {
    });
    assertEquals(Arrays.asList(1, 2, 3), returnedBrokerIds.getBrokers());
  }

  @Test
  public void testAuthenticationError() {
    expect(brokerManager.listLocalBrokers())
        .andReturn(
            failedFuture(
                new SaslAuthenticationException(Errors.SASL_AUTHENTICATION_FAILED.message())));
    replay(brokerManager);

    Response response = request("/brokers", Versions.KAFKA_V2_JSON).get();
    assertErrorResponse(Response.Status.UNAUTHORIZED, response,
        io.confluent.kafkarest.Errors.KAFKA_AUTHENTICATION_ERROR_CODE,
        Errors.SASL_AUTHENTICATION_FAILED.message(),
        Versions.KAFKA_V2_JSON);
  }
}
