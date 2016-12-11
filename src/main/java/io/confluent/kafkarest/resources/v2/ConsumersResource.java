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
package io.confluent.kafkarest.resources.v2;

import java.util.List;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.UriInfo;

import io.confluent.kafkarest.v2.KafkaConsumerState;
import io.confluent.kafkarest.v2.BinaryKafkaConsumerState;
import io.confluent.kafkarest.v2.AvroKafkaConsumerState;
import io.confluent.kafkarest.v2.JsonKafkaConsumerState;
import io.confluent.kafkarest.v2.KafkaConsumerManager;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.UriUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/consumers")
// We include embedded formats here so you can always use these headers when interacting with
// a consumers resource. The few cases where it isn't safe are overridden per-method
@Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW, Versions.KAFKA_V2_JSON_WEIGHTED
           })
@Consumes({Versions.KAFKA_V2_JSON_BINARY, Versions.KAFKA_V2_JSON_AVRO, Versions.KAFKA_V2_JSON_JSON,
           Versions.KAFKA_V2_JSON
	    })
public class ConsumersResource {

  private final Context ctx;

  public ConsumersResource(Context ctx) {
    this.ctx = ctx;
  }

  @POST
  @Valid
  @Path("/{group}")
  @PerformanceMetric("consumer_v2.create")
  public CreateConsumerInstanceResponse createGroup(
      @javax.ws.rs.core.Context UriInfo uriInfo, final @PathParam("group") String group,
      @Valid ConsumerInstanceConfig config) {
    if (config == null) {
      config = new ConsumerInstanceConfig();
    }
    String instanceId = ctx.getKafkaConsumerManager().createConsumer(group, config);
    String instanceBaseUri = UriUtils.absoluteUriBuilder(ctx.getConfig(), uriInfo)
        .path("instances").path(instanceId).build().toString();
    return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer_v2.delete")
  public void deleteGroup(final @PathParam("group") String group,
                          final @PathParam("instance") String instance) {
    ctx.getKafkaConsumerManager().deleteConsumer(group, instance);
  }

  @POST
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer_v2.subscribe")
  public void Subscribe(
			 final @PathParam("group") String group,
			 final @PathParam("instance") String instance) {
      System.out.println("============ Calling v2 subscription ============");
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer_v2.records.read-binary")
  @Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V2_JSON_WEIGHTED
	      })
  public void readRecordBinary(final @Suspended AsyncResponse asyncResponse,
                              final @PathParam("group") String group,
                              final @PathParam("instance") String instance,
                              @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
      System.out.println("============ Calling v2 readRecordBinary ===========");
      String topic="test";
      //readTopic(asyncResponse, group, instance, topic, maxBytes, BinaryKafkaConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer_v2.records.read-json")
  @Produces({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW}) // Using low weight ensures binary is default
  public void readRecordJson(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
      System.out.println("============ Calling v2 readRecordJson ===========");            
      //readTopic(asyncResponse, group, instance, topic, maxBytes, JsonKafakaConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer_v2.records.read-avro")
  @Produces({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW}) // Using low weight ensures binary is default
  public void readRecordAvro(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
      System.out.println("============ Calling v2 readRecordAvro ===========");            
      //readTopic(asyncResponse, group, instance, topic, maxBytes, AvroKafkaConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer_v2.topic.read-binary")
  @Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V2_JSON_WEIGHTED
	      })
  public void readTopicBinary(final @Suspended AsyncResponse asyncResponse,
                              final @PathParam("group") String group,
                              final @PathParam("instance") String instance,
                              final @PathParam("topic") String topic,
                              @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
    readTopic(asyncResponse, group, instance, topic, maxBytes, BinaryKafkaConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer_v2.topic.read-json")
  @Produces({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW}) // Using low weight ensures binary is default
  public void readTopicJson(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @PathParam("topic") String topic,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
    readTopic(asyncResponse, group, instance, topic, maxBytes, JsonKafkaConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer_v2.topic.read-avro")
  @Produces({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW}) // Using low weight ensures binary is default
  public void readTopicAvro(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @PathParam("topic") String topic,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
    readTopic(asyncResponse, group, instance, topic, maxBytes, AvroKafkaConsumerState.class);
  }

    
  private <KafkaK, KafkaV, ClientK, ClientV> void readTopic(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      Class<? extends KafkaConsumerState<KafkaK, KafkaV, ClientK, ClientV>> consumerStateType) {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
    ctx.getKafkaConsumerManager().readTopic(
        group, instance, topic, consumerStateType, maxBytes,
        new KafkaConsumerManager.ReadCallback<ClientK, ClientV>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<ClientK, ClientV>> records,
                                   Exception e) {
            if (e != null) {
              asyncResponse.resume(e);
            } else {
              asyncResponse.resume(records);
            }
          }
        });
  }

    
}
