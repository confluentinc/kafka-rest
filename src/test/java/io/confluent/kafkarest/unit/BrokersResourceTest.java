/**
 * Copyright 2015 Confluent Inc.
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
 **/
package io.confluent.kafkarest.unit;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.BrokerList;
import io.confluent.kafkarest.resources.BrokersResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class BrokersResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private AdminClientWrapper adminClientWrapper;
  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  public BrokersResourceTest() throws RestConfigException {
    adminClientWrapper = EasyMock.createMock(AdminClientWrapper.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config,
        null,
        producerPool,
        null,
        null,
        null,
        adminClientWrapper
    );
    addResource(new BrokersResource(ctx));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(adminClientWrapper, producerPool);
  }

  @Test
  public void testList() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      final List<Integer> brokerIds = Arrays.asList(1, 2, 3);
      EasyMock.expect(adminClientWrapper.getBrokerIds()).andReturn(brokerIds);
      EasyMock.replay(adminClientWrapper);

      Response response = request("/brokers", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      final BrokerList returnedBrokerIds = TestUtils.tryReadEntityOrLog(response, new GenericType<BrokerList>() {
      });
      assertEquals(brokerIds, returnedBrokerIds.getBrokers());
      EasyMock.verify(adminClientWrapper);
      EasyMock.reset(adminClientWrapper, producerPool);
    }
  }
}
