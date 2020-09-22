/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MappingIterator;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/records")
public final class ProduceToPartitionAction {

  private static final Logger log = LoggerFactory.getLogger(ProduceToPartitionAction.class);

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response produce(
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") int partitionId,
      MappingIterator<Foo> request) {
    ChunkedOutput<Bar> output = new ChunkedOutput<>(Bar.class, "\r\n");
    new Thread(
        () -> {
          try {
            while (request.hasNext()) {
              Foo foo = request.next();
              log.info(foo.toString());
              output.write(Bar.create(foo.foo()));
            }
            output.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).start();
    return Response.ok(output).encoding("chunked").build();
  }

  @AutoValue
  public abstract static class Foo {

    Foo() {
    }

    @JsonProperty("foo")
    public abstract String foo();

    @JsonCreator
    static Foo fromJson(@JsonProperty("foo") String foo) {
      return create(foo);
    }

    public static Foo create(String foo) {
      return new AutoValue_ProduceToPartitionAction_Foo(foo);
    }
  }

  @AutoValue
  public abstract static class Bar {

    Bar() {
    }

    @JsonProperty("bar")
    public abstract String bar();

    @JsonCreator
    static Bar fromJson(@JsonProperty("bar") String bar) {
      return create(bar);
    }

    public static Bar create(String bar) {
      return new AutoValue_ProduceToPartitionAction_Bar(bar);
    }
  }
}
