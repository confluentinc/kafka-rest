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

import io.confluent.kafkarest.*;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.resources.ConsumersResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class AbstractConsumerResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  protected MetadataObserver mdObserver;
  protected ConsumerManager consumerManager;
  protected Context ctx;

  protected static final String groupName = "testgroup";
  protected static final String topicName = "testtopic";
  protected static final String secondTopicName = "testtopic2";
  protected static final String instanceId = "uniqueid";
  protected static final String instancePath
      = "/consumers/" + groupName + "/instances/" + instanceId;

  protected static final String not_found_message = "not found";

  public AbstractConsumerResourceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    consumerManager = EasyMock.createMock(ConsumerManager.class);
    ctx = new Context(config, mdObserver, null, consumerManager, null);

    addResource(new ConsumersResource(ctx));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(mdObserver, consumerManager);
  }

  protected void expectCreateGroup(ConsumerInstanceConfig config) {
    EasyMock.expect(consumerManager.createConsumer(
        EasyMock.eq(groupName), EasyMock.eq(config)))
        .andReturn(instanceId);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV> void expectReadTopic(
      String topicName, Class<? extends ConsumerState<KafkaK, KafkaV, ClientK, ClientV>> stateClass,
      final List<? extends AbstractConsumerRecord<ClientK, ClientV>> readResult,
      final Exception readException) {
    expectReadTopic(topicName, stateClass, Long.MAX_VALUE, readResult, readException);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV> void expectReadTopic(
      String topicName, Class<? extends ConsumerState<KafkaK, KafkaV, ClientK, ClientV>> stateClass,
      long maxBytes, final List<? extends AbstractConsumerRecord<ClientK, ClientV>> readResult,
      final Exception readException) {
    final Capture<ConsumerManager.ReadCallback>
        readCallback =
        new Capture<ConsumerManager.ReadCallback>();
    consumerManager
        .readTopic(EasyMock.eq(groupName), EasyMock.eq(instanceId), EasyMock.eq(topicName),
                   EasyMock.eq(stateClass), EasyMock.eq(maxBytes), EasyMock.capture(readCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        readCallback.getValue().onCompletion(readResult, readException);
        return null;
      }
    });
  }

  protected void expectCommit(final List<TopicPartitionOffset> commitResult,
                              final Exception commitException) {
    final Capture<ConsumerManager.CommitCallback>
        commitCallback =
        new Capture<ConsumerManager.CommitCallback>();
    consumerManager.commitOffsets(EasyMock.eq(groupName), EasyMock.eq(instanceId),
                                  EasyMock.capture(commitCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        commitCallback.getValue().onCompletion(commitResult, commitException);
        return null;
      }
    });
  }

  protected String instanceBasePath(CreateConsumerInstanceResponse createResponse) {
    try {
      return new URI(createResponse.getBaseUri()).getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "Invalid URI in CreateConsumerInstanceResponse: \"" + createResponse.getBaseUri() + "\"");
    }
  }

  protected void expectDeleteGroup(boolean invalid) {
    consumerManager.deleteConsumer(groupName, instanceId);
    IExpectationSetters expectation = EasyMock.expectLastCall();
    if (invalid) {
      expectation.andThrow(new RestNotFoundException(not_found_message, 1000));
    }
  }
}
