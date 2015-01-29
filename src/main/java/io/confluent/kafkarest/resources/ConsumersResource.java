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

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/consumers")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class ConsumersResource {

  private final Context ctx;

  public ConsumersResource(Context ctx) {
    this.ctx = ctx;
  }

  @POST
  @Valid
  @Path("/{group}")
  @PerformanceMetric("consumer.create")
  public CreateConsumerInstanceResponse createGroup(
      @javax.ws.rs.core.Context UriInfo uriInfo, final @PathParam("group") String group,
      @Valid ConsumerInstanceConfig config) {
    if (config == null) {
      config = new ConsumerInstanceConfig();
    }
    String instanceId = ctx.getConsumerManager().createConsumer(group, config);
    String
        instanceBaseUri =
        uriInfo.getAbsolutePathBuilder().path("instances").path(instanceId).build().toString();
    return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
  }

  @POST
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.commit")
  public void commitOffsets(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance) {
    ctx.getConsumerManager().commitOffsets(group, instance, new ConsumerManager.CommitCallback() {
      @Override
      public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
        if (e != null) {
          asyncResponse.resume(e);
        } else {
          asyncResponse.resume(offsets);
        }
      }
    });
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.delete")
  public void deleteGroup(final @PathParam("group") String group,
                          final @PathParam("instance") String instance) {
    ctx.getConsumerManager().deleteConsumer(group, instance);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer.topic.read")
  public void readTopic(final @Suspended AsyncResponse asyncResponse,
                        final @PathParam("group") String group,
                        final @PathParam("instance") String instance,
                        final @PathParam("topic") String topic,
                        @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes)
  {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
    ctx.getConsumerManager().readTopic(group, instance, topic, maxBytes,
                                       new ConsumerManager.ReadCallback() {
      @Override
      public void onCompletion(List<ConsumerRecord> records, Exception e) {
        if (e != null) {
          asyncResponse.resume(e);
        } else {
          asyncResponse.resume(records);
        }
      }
    });
  }
}
