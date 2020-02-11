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

package io.confluent.kafkarest.resources.v1;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.BrokerList;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class BrokersResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private AdminClient adminClient;
  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  public BrokersResourceTest() throws RestConfigException {
    adminClient = EasyMock.createMock(AdminClient.class);
    AdminClientWrapper adminClientWrapper = new AdminClientWrapper(new KafkaRestConfig(new Properties()), adminClient);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config,
        producerPool,
        null,
        adminClientWrapper,
        null
    );
    addResource(new BrokersResource(ctx));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(adminClient, producerPool);
  }

  @Test
  public void testList() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      final Collection<Node> nodes = Arrays.asList(new Node(1, "host1", 1),
          new Node(2, "host2", 2),
          new Node(3, "host3", 3));
      DescribeClusterResult describeClusterResult = EasyMock.createMock(DescribeClusterResult.class);

      KafkaFutureImpl<Collection<Node>> nodesFuture = new KafkaFutureImpl<>();
      nodesFuture.complete(nodes);
      EasyMock.expect(describeClusterResult.nodes()).andReturn(nodesFuture);
      EasyMock.expect(adminClient.describeCluster()).andReturn(describeClusterResult);

      EasyMock.replay(describeClusterResult);
      EasyMock.replay(adminClient);

      Response response = request("/brokers", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      final BrokerList returnedBrokerIds = TestUtils.tryReadEntityOrLog(response, new GenericType<BrokerList>() {
      });
      assertEquals(Arrays.asList(1, 2, 3), returnedBrokerIds.getBrokers());
      EasyMock.verify(adminClient);
      EasyMock.reset(adminClient, producerPool);
    }
  }

  @Test
  public void testAuthenticationError() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      DescribeClusterResult describeClusterResult = EasyMock.createMock(DescribeClusterResult.class);

      KafkaFutureImpl<Collection<Node>> nodes = new KafkaFutureImpl<>();
      nodes.completeExceptionally(new SaslAuthenticationException(Errors.SASL_AUTHENTICATION_FAILED.message()));
      EasyMock.expect(describeClusterResult.nodes()).andReturn(nodes);
      EasyMock.expect(adminClient.describeCluster()).andReturn(describeClusterResult);

      EasyMock.replay(describeClusterResult);
      EasyMock.replay(adminClient);

      Response response = request("/brokers", mediatype.header).get();

      assertErrorResponse(Response.Status.UNAUTHORIZED, response,
          io.confluent.kafkarest.Errors.KAFKA_AUTHENTICATION_ERROR_CODE,
          Errors.SASL_AUTHENTICATION_FAILED.message(),
          mediatype.expected);

      EasyMock.verify(adminClient);
      EasyMock.reset(adminClient, producerPool);
    }
  }
}
