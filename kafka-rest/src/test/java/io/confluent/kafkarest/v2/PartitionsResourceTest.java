/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafkarest.v2;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.resources.v2.PartitionsResource;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;

public class PartitionsResourceTest extends JerseyTest {

  private static final String TOPIC = "topic";
  private static final int PARTITION = 1;
  private static final long BEGINNING_OFFSET = 10L;
  private static final long END_OFFSET = 20L;

  private AdminClientWrapper adminClientWrapper;
  private KafkaConsumerManager consumerManager;

  @Override
  protected Application configure() {
    ResourceConfig application = new ResourceConfig();
    application.register(new PartitionsResource(createKafkaRestContext()));
    application.register(new JacksonMessageBodyProvider());
    return application;
  }

  private KafkaRestContext createKafkaRestContext() {
    adminClientWrapper = createMock(AdminClientWrapper.class);
    consumerManager = createMock(KafkaConsumerManager.class);

    try {
      return new DefaultKafkaRestContext(
          new KafkaRestConfig(),
          /* producerPool= */ null,
          consumerManager,
          adminClientWrapper,
          /* groupMetadataObserver= */ null,
          /* scalaConsumersContext= */ null);
    } catch (RestConfigException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUpMocks() {
    reset(adminClientWrapper, consumerManager);
  }

  @Test
  public void getOffsets_returnsBeginningAndEnd() throws Exception {
    expect(adminClientWrapper.topicExists(TOPIC)).andReturn(true);
    expect(adminClientWrapper.partitionExists(TOPIC, PARTITION)).andReturn(true);
    expect(consumerManager.getBeginningOffset(TOPIC, PARTITION)).andReturn(BEGINNING_OFFSET);
    expect(consumerManager.getEndOffset(TOPIC, PARTITION)).andReturn(END_OFFSET);
    replay(adminClientWrapper, consumerManager);

    String response =
        target("/topics/{topic}/partitions/{partition}/offsets")
            .resolveTemplate("topic", TOPIC)
            .resolveTemplate("partition", PARTITION)
            .request()
            .get(String.class);

    assertEquals(
        String.format("{\"beginning_offset\":%d,\"end_offset\":%d}", BEGINNING_OFFSET, END_OFFSET),
        response);

    verify(adminClientWrapper, consumerManager);
  }

  @Test
  public void getOffsets_topicDoesNotExist_returns404() throws Exception {
    expect(adminClientWrapper.topicExists(TOPIC)).andReturn(false);
    replay(adminClientWrapper);

    Response response =
        target("/topics/{topic}/partitions/{partition}/offsets")
            .resolveTemplate("topic", TOPIC)
            .resolveTemplate("partition", PARTITION)
            .request()
            .get();

    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    verify(adminClientWrapper);
  }

  @Test
  public void getOffsets_partitionDoesNotExist_returns404() throws Exception {
    expect(adminClientWrapper.topicExists(TOPIC)).andReturn(true);
    expect(adminClientWrapper.partitionExists(TOPIC, PARTITION)).andReturn(false);
    replay(adminClientWrapper);

    Response response =
        target("/topics/{topic}/partitions/{partition}/offsets")
            .resolveTemplate("topic", TOPIC)
            .resolveTemplate("partition", PARTITION)
            .request()
            .get();

    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    verify(adminClientWrapper);
  }
}
