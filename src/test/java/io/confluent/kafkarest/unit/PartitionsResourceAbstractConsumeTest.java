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
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import java.util.List;

public class PartitionsResourceAbstractConsumeTest extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  protected final String topicName = "topic1";
  protected final int partitionId = 0;
  protected final long offset = 0;
  protected final long count = 1;

  protected final SimpleConsumerManager simpleConsumerManager;

  public PartitionsResourceAbstractConsumeTest() throws RestConfigException {
    super();
    simpleConsumerManager = EasyMock.createMock(SimpleConsumerManager.class);

    final Context ctx = new Context(config, null, null, null, simpleConsumerManager);
    addResource(new PartitionsResource(ctx));
  }

  protected void expectConsume(final EmbeddedFormat embeddedFormat,  final List<? extends AbstractConsumerRecord> records) {
    final Capture<ConsumerManager.ReadCallback> readCallback = new Capture<ConsumerManager.ReadCallback>();
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
