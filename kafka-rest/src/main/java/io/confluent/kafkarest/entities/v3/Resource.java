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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class Resource {

  Resource() {
  }

  @JsonProperty("kind")
  public abstract String getKind();

  @JsonProperty("metadata")
  public abstract Metadata getMetadata();

  public abstract static class Builder<BuilderT extends Builder<BuilderT>> {

    Builder() {
    }

    public abstract BuilderT setKind(String kind);

    public abstract BuilderT setMetadata(Metadata metadata);
  }

  @AutoValue
  public abstract static class Metadata {

    Metadata() {
    }

    @JsonProperty("self")
    public abstract String getSelf();

    @JsonProperty("resource_name")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<String> getResourceName();

    public static Builder builder() {
      return new AutoValue_Resource_Metadata.Builder();
    }

    @JsonCreator
    static Metadata fromJson(
        @JsonProperty("self") String self,
        @JsonProperty("resource_name") @Nullable String resourceName) {
      return builder()
          .setSelf(self)
          .setResourceName(resourceName)
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {
      }

      public abstract Builder setSelf(String self);

      public abstract Builder setResourceName(@Nullable String resourceName);

      public abstract Metadata build();
    }
  }

  @AutoValue
  public abstract static class Relationship {

    Relationship() {
    }

    @JsonProperty("related")
    public abstract String getRelated();

    public static Relationship create(String related) {
      return new AutoValue_Resource_Relationship(related);
    }

    @JsonCreator
    static Relationship fromJson(@JsonProperty("related") String related) {
      return create(related);
    }
  }
}
