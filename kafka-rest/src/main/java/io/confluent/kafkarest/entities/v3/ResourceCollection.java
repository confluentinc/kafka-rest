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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class ResourceCollection<ResourceT extends Resource> {

  ResourceCollection() {
  }

  @JsonProperty("kind")
  public abstract String getKind();

  @JsonProperty("metadata")
  public abstract Metadata getMetadata();

  @JsonProperty("data")
  public abstract ImmutableList<ResourceT> getData();

  public abstract static class Builder<
      ResourceT extends Resource,
      ResourceCollectionT extends ResourceCollection<ResourceT>,
      BuilderT extends Builder<ResourceT, ResourceCollectionT, BuilderT>> {

    Builder() {
    }

    public abstract BuilderT setKind(String kind);

    public abstract BuilderT setMetadata(Metadata metadata);

    public abstract BuilderT setData(List<ResourceT> data);

    public abstract ResourceCollectionT build();
  }

  @AutoValue
  public abstract static class Metadata {

    Metadata() {
    }

    @JsonProperty("self")
    public abstract String getSelf();

    @JsonProperty("next")
    public abstract Optional<String> getNext();

    public static Builder builder() {
      return new AutoValue_ResourceCollection_Metadata.Builder();
    }

    @JsonCreator
    static Metadata fromJson(
        @JsonProperty("self") String self, @JsonProperty("next") @Nullable String next) {
      return builder()
          .setSelf(self)
          .setNext(next)
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {
      }

      public abstract Builder setSelf(String self);

      public abstract Builder setNext(@Nullable String next);

      public abstract Metadata build();
    }
  }
}
