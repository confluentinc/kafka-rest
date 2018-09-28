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

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.ConsumerState;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.extension.ContextInvocationHandler;
import io.confluent.kafkarest.integration.TestContextProviderFilter;
import io.confluent.kafkarest.resources.ConsumersResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestException;

public class AbstractConsumerResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  protected MetadataObserver mdObserver;
  protected ConsumerManager consumerManager;
  protected DefaultKafkaRestContext ctx;

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
    ctx = new DefaultKafkaRestContext(config, mdObserver, null, consumerManager, null, null,
        null);
    ContextInvocationHandler contextInvocationHandler = new ContextInvocationHandler();
    KafkaRestContext contextProxy =
        (KafkaRestContext) Proxy.newProxyInstance(KafkaRestContext.class.getClassLoader(), new
            Class[]{KafkaRestContext
                        .class}, contextInvocationHandler);

    addResource(new ConsumersResource(contextProxy));
    addResource(new TestContextProviderFilter(ctx));
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
      final List<? extends ConsumerRecord<ClientK, ClientV>> readResult,
      final Exception readException) {
    expectReadTopic(topicName, stateClass, Long.MAX_VALUE, readResult, readException);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV> void expectReadTopic(
      String topicName, Class<? extends ConsumerState<KafkaK, KafkaV, ClientK, ClientV>> stateClass,
      long maxBytes, final List<? extends ConsumerRecord<ClientK, ClientV>> readResult,
      final Exception readException) {
    final Capture<ConsumerReadCallback>
        readCallback =
        new Capture<ConsumerReadCallback>();
    consumerManager
        .readTopic(EasyMock.eq(groupName), EasyMock.eq(instanceId), EasyMock.eq(topicName),
                   EasyMock.eq(stateClass), EasyMock.eq(maxBytes), EasyMock.capture(readCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() {
        RestException e = null;
        if (readException != null) {
          if (!(readException instanceof RestException)) {
            e = Errors.kafkaErrorException(readException);
          } else {
            e = (RestException) readException;
          }
        }
        readCallback.getValue().onCompletion(readResult, e);
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
