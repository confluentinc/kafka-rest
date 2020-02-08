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

package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ScalaConsumersContext;
import io.confluent.kafkarest.SimpleConsumerManager;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.extension.InstantConverterProvider;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.util.List;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;

public class PartitionsResourceAbstractConsumeTest extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  protected final String topicName = "topic1";
  protected final int partitionId = 0;
  protected final long offset = 0;
  protected final long count = 1;

  protected final SimpleConsumerManager simpleConsumerManager;

  public PartitionsResourceAbstractConsumeTest() throws RestConfigException {
    super();
    simpleConsumerManager = EasyMock.createMock(SimpleConsumerManager.class);

    final ScalaConsumersContext scalaConsumersContext = new ScalaConsumersContext(null, null, simpleConsumerManager);
    final DefaultKafkaRestContext ctx = new DefaultKafkaRestContext(config, null,
            null, null, null, scalaConsumersContext);
    addResource(new PartitionsResource(ctx));
    addResource(InstantConverterProvider.class);
  }

  protected void expectConsume(final EmbeddedFormat embeddedFormat,  final List<? extends ConsumerRecord> records) {
    final Capture<ConsumerReadCallback> readCallback = Capture.newInstance();
    simpleConsumerManager.consume(
        EasyMock.eq(topicName),
        EasyMock.eq(partitionId),
        EasyMock.eq(offset),
        EasyMock.eq(count),
        EasyMock.eq(embeddedFormat),
        EasyMock.capture(readCallback));

    EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override
      public Void answer() throws Throwable {
        readCallback.getValue().onCompletion(records, null);
        return null;
      }
    });

    EasyMock.replay(simpleConsumerManager);
  }

  protected Response request(String topicName, int partitionId, long offset, String mediaType) {
    Invocation.Builder builder = getJerseyTest()
        .target("/topics/" + topicName + "/partitions/" + partitionId + "/messages")
        .queryParam("offset", offset)
        .request();

    if (mediaType != null) {
      builder.accept(new String[]{mediaType});
    }

    return builder.get();
  }

}
