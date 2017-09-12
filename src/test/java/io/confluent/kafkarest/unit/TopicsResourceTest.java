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

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class TopicsResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private AdminClientWrapper adminClientWrapper;
  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  public TopicsResourceTest() throws RestConfigException {
    adminClientWrapper = EasyMock.createMock(AdminClientWrapper.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config, null, producerPool, null, null,
        null,  adminClientWrapper);

    addResource(new TopicsResource(ctx));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(adminClientWrapper, producerPool);
  }

  @Test
  public void testList() {
    final List<String> topics = Arrays.asList("test1", "test2", "test3");
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      EasyMock.expect(adminClientWrapper.getTopicNames()).andReturn(topics);
      EasyMock.replay(adminClientWrapper);

      Response response = request("/topics", mediatype.expected).get();
      assertOKResponse(response, mediatype.expected);
      final List<String> topicsResponse = TestUtils.tryReadEntityOrLog(response, new GenericType<List<String>>() {
      });
      assertEquals(topics, topicsResponse);

      EasyMock.verify(adminClientWrapper);
      EasyMock.reset(adminClientWrapper);
    }
  }

  @Test
  public void testGetTopic() {
    Properties nonEmptyConfig = new Properties();
    nonEmptyConfig.setProperty("cleanup.policy", "delete");
    final List<Partition> partitions1 = Arrays.asList(
        new Partition(0, 0, Arrays.asList(
            new PartitionReplica(0, true, true),
            new PartitionReplica(1, false, false)
        )),
        new Partition(1, 1, Arrays.asList(
            new PartitionReplica(0, false, true),
            new PartitionReplica(1, true, true)
        ))
    );
    final List<Partition> partitions2 = Arrays.asList(
        new Partition(0, 0, Arrays.asList(
            new PartitionReplica(0, true, true),
            new PartitionReplica(1, false, false)
        ))
    );
    Topic topic1 = new Topic("topic1", new Properties(), partitions1);
    Topic topic2 = new Topic("topic2", nonEmptyConfig, partitions2);

    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      EasyMock.expect(adminClientWrapper.getTopic("topic1"))
          .andReturn(topic1);
      EasyMock.expect(adminClientWrapper.getTopic("topic2"))
          .andReturn(topic2);
      EasyMock.replay(adminClientWrapper);

      Response response1 = request("/topics/topic1", mediatype.header).get();
      assertOKResponse(response1, mediatype.expected);
      final Topic topicResponse1 = TestUtils.tryReadEntityOrLog(response1, new GenericType<Topic>() {
      });
      assertEquals(topic1, topicResponse1);

      Response response2 = request("/topics/topic2", mediatype.header).get();
      final Topic topicResponse2 = TestUtils.tryReadEntityOrLog(response2, new GenericType<Topic>() {
      });
      assertEquals(topic2, topicResponse2);

      EasyMock.verify(adminClientWrapper);
      EasyMock.reset(adminClientWrapper);
    }
  }

  @Test
  public void testGetInvalidTopic() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      EasyMock.expect(adminClientWrapper.getTopic("nonexistanttopic"))
          .andReturn(null);
      EasyMock.replay(adminClientWrapper);

      Response response = request("/topics/nonexistanttopic", mediatype.header).get();
      assertErrorResponse(Response.Status.NOT_FOUND, response,
                          Errors.TOPIC_NOT_FOUND_ERROR_CODE, Errors.TOPIC_NOT_FOUND_MESSAGE,
                          mediatype.expected);

      EasyMock.verify(adminClientWrapper);
      EasyMock.reset(adminClientWrapper);
    }
  }
}
