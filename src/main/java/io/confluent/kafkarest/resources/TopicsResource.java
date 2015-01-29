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
package io.confluent.kafkarest.resources;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.ProducerRecordProxyCollection;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicProduceRequest;

@Path("/topics")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class TopicsResource {

  private final Context ctx;

  public TopicsResource(Context ctx) {
    this.ctx = ctx;
  }

  @GET
  public Collection<String> list() {
    return ctx.getMetadataObserver().getTopicNames();
  }

  @GET
  @Path("/{topic}")
  public Topic getTopic(@PathParam("topic") String topicName) {
    Topic topic = ctx.getMetadataObserver().getTopic(topicName);
    if (topic == null) {
      throw Errors.topicNotFoundException();
    }
    return topic;
  }

  @Path("/{topic}/partitions")
  public PartitionsResource getPartitions(@PathParam("topic") String topicName) {
    return new PartitionsResource(ctx, topicName);
  }

  @POST
  @Path("/{topic}")
  public void produce(final @Suspended AsyncResponse asyncResponse,
                      @PathParam("topic") String topicName, @Valid TopicProduceRequest request) {
    if (!ctx.getMetadataObserver().topicExists(topicName)) {
      throw Errors.topicNotFoundException();
    }

    ctx.getProducerPool().produce(
        new ProducerRecordProxyCollection(topicName, request.getRecords()),
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(Map<Integer, Long> partitionOffsets) {
            ProduceResponse response = new ProduceResponse();
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (Map.Entry<Integer, Long> partOff : partitionOffsets.entrySet()) {
              offsets.add(new PartitionOffset(partOff.getKey(), partOff.getValue()));
            }
            response.setOffsets(offsets);
            asyncResponse.resume(response);
          }

          public void onException(Exception e) {
            asyncResponse.resume(e);
          }
        }
    );
  }
}
