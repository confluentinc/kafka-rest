/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.integration;

import static java.util.Collections.singletonList;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.backends.kafka.KafkaModule;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.exceptions.RestException;
import java.util.Properties;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

public class KafkaRestContextIntegrationTest extends ClusterTestHarness {

  /** HTTP 418 I'm a teapot */
  private static final int I_M_A_TEAPOT_STATUS_CODE = 418;

  public KafkaRestContextIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(
        KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG,
        singletonList(KafkaRestContextExtension.class));
  }

  @Test
  public void withoutRequestContextProperty_defaultContextIsUsed() {
    Response response = request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void withRequestContextProperty_contextFromRequestIsUsed() {
    Response response =
        request("/v3/clusters")
            .header("X-Teapot-Context", "")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(I_M_A_TEAPOT_STATUS_CODE, response.getStatus());
  }

  public static final class KafkaRestContextExtension implements RestResourceExtension {

    @Override
    public void register(Configurable<?> configurable, KafkaRestConfig config) {
      configurable.register(KafkaRestContextRequestFilter.class);
    }

    @Override
    public void clean() {}
  }

  private static final class KafkaRestContextRequestFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext requestContext) {
      if (requestContext.getHeaderString("X-Teapot-Context") != null) {
        requestContext.setProperty(
            KafkaModule.KAFKA_REST_CONTEXT_PROPERTY_NAME, new TeapotKafkaRestContext());
      }
    }
  }

  private static final class TeapotKafkaRestContext implements KafkaRestContext {

    @Override
    public KafkaRestConfig getConfig() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ProducerPool getProducerPool() {
      throw new UnsupportedOperationException();
    }

    @Override
    public KafkaConsumerManager getKafkaConsumerManager() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Admin getAdmin() {
      Admin adminClient = mock(Admin.class);
      DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
      expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
      expect(describeClusterResult.clusterId())
          .andReturn(
              KafkaFutures.failedFuture(
                  new RestException(
                      "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE)));
      expect(describeClusterResult.controller())
          .andReturn(
              KafkaFutures.failedFuture(
                  new RestException(
                      "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE)));
      expect(describeClusterResult.nodes())
          .andReturn(
              KafkaFutures.failedFuture(
                  new RestException(
                      "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE)));
      replay(adminClient, describeClusterResult);
      return adminClient;
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {}
  }
}
