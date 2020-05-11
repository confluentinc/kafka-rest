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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * An outbound relationship between two resource types.
 */
public final class Relationship {

  private final Link links;

  @Nullable
  private final Data data;

  public Relationship(String related) {
    this(new Link(related), null);
  }

  public Relationship(String related,
                      String type,
                      String id) {
    this(new Link(related), new Data(type, id));
  }

  @JsonCreator
  public Relationship(@JsonProperty("links") Link link,
                      @JsonProperty("data") @Nullable Data data) {
    this.links = link;
    this.data = data;
  }

  @JsonProperty("links")
  public Link getLinks() {
    return links;
  }

  @JsonProperty("data")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Data getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Relationship that = (Relationship) o;
    return Objects.equals(links, that.links) && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(links, data);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Relationship.class.getSimpleName() + "[", "]")
        .add("links=" + links)
        .add("data=" + data)
        .toString();
  }

  public static final class Link {

    private final String related;

    @JsonCreator
    private Link(@JsonProperty("related") String related) {
      this.related = Objects.requireNonNull(related);
    }

    @JsonProperty("related")
    public String getRelated() {
      return related;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Link link = (Link) o;
      return Objects.equals(related, link.related);
    }

    @Override
    public int hashCode() {
      return Objects.hash(related);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Link.class.getSimpleName() + "[", "]")
          .add("related='" + related + "'")
          .toString();
    }
  }

  public static final class Data {

    private final String type;

    private final String id;

    @JsonCreator
    private Data(@JsonProperty("type") String type,
                 @JsonProperty("id") String id) {
      this.type = Objects.requireNonNull(type);
      this.id = Objects.requireNonNull(id);
    }

    @JsonProperty("type")
    public String getType() {
      return type;
    }

    @JsonProperty("id")
    public String getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Data data = (Data) o;
      return Objects.equals(type, data.type) && Objects.equals(id, data.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, id);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Data.class.getSimpleName() + "[", "]")
          .add("type='" + type + "'")
          .add("id='" + id + "'")
          .toString();
    }
  }
}
