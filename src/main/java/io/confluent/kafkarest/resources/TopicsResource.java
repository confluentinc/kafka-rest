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
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.entities.TopicProduceResponse;
import io.confluent.rest.annotations.PerformanceMetric;

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
  @PerformanceMetric("topics.list")
  public Collection<String> list() {
    return ctx.getMetadataObserver().getTopicNames();
  }

  @GET
  @Path("/{topic}")
  @PerformanceMetric("topic.get")
  public Topic getTopic(@PathParam("topic") String topicName) {
    Topic topic = ctx.getMetadataObserver().getTopic(topicName);
    if (topic == null) {
      throw Errors.topicNotFoundException();
    }
    return topic;
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-binary")
  @Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
             Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST})
  public void produceBinary(final @Suspended AsyncResponse asyncResponse,
                            @PathParam("topic") String topicName,
                            @Valid TopicProduceRequest<BinaryTopicProduceRecord> request) {
    produce(asyncResponse, topicName, Versions.EmbeddedFormat.BINARY, request);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-avro")
  @Consumes({Versions.KAFKA_V1_JSON_AVRO})
  public void produceAvro(final @Suspended AsyncResponse asyncResponse,
                          @PathParam("topic") String topicName,
                          @Valid TopicProduceRequest<AvroTopicProduceRecord> request) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false, hasValues = false;
    for (AvroTopicProduceRecord rec : request.getRecords()) {
      hasKeys = hasKeys || (rec.getJsonKey() != null);
      hasValues = hasValues || (rec.getJsonValue() != null);
    }
    if (hasKeys && request.getKeySchema() == null && request.getKeySchemaId() == null) {
      throw Errors.keySchemaMissingException();
    }
    if (hasValues && request.getValueSchema() == null && request.getValueSchemaId() == null) {
      throw Errors.valueSchemaMissingException();
    }

    produce(asyncResponse, topicName, Versions.EmbeddedFormat.AVRO, request);
  }

  public <K, V, R extends TopicProduceRecord<K, V>> void produce(
      final AsyncResponse asyncResponse,
      final String topicName,
      final Versions.EmbeddedFormat format,
      final TopicProduceRequest<R> request) {
    if (!ctx.getMetadataObserver().topicExists(topicName)) {
      throw Errors.topicNotFoundException();
    }

    ctx.getProducerPool().produce(
        topicName, null, format,
        request,
        request.getRecords(),
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(Integer keySchemaId, Integer valueSchemaId,
                                   Map<Integer, Long> partitionOffsets) {
            TopicProduceResponse response = new TopicProduceResponse();
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (Map.Entry<Integer, Long> partOff : partitionOffsets.entrySet()) {
              offsets.add(new PartitionOffset(partOff.getKey(), partOff.getValue()));
            }
            response.setOffsets(offsets);
            response.setKeySchemaId(keySchemaId);
            response.setValueSchemaId(valueSchemaId);
            asyncResponse.resume(response);
          }

          public void onException(Exception e) {
            asyncResponse.resume(e);
          }
        }
    );
  }
}
