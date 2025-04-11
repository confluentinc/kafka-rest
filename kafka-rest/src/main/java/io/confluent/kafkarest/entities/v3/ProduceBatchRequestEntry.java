/*
 * Copyright 2023 Confluent Inc.
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

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestHeader;
import io.confluent.kafkarest.resources.v3.ProduceBatchAction;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
@JsonDeserialize(using = ProduceBatchRequestEntry.Deserializer.class)
public abstract class ProduceBatchRequestEntry {

  ProduceBatchRequestEntry() {}

  @JsonProperty("id")
  public abstract JsonNode getId();

  @JsonProperty("partition_id")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<Integer> getPartitionId();

  @JsonProperty("headers")
  @JsonInclude(Include.NON_EMPTY)
  public abstract ImmutableList<ProduceRequestHeader> getHeaders();

  @JsonProperty("key")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceRequestData> getKey();

  @JsonProperty("value")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceRequestData> getValue();

  @JsonProperty("timestamp")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<Instant> getTimestamp();

  @JsonIgnore
  public abstract long getOriginalSize();

  public static Builder builder() {
    return new AutoValue_ProduceBatchRequestEntry.Builder().setHeaders(emptyList());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    @JsonCreator
    static Builder fromJson(
        @JsonProperty("id") JsonNode id,
        @JsonProperty("partition_id") @Nullable Integer partitionId,
        @JsonProperty("headers") @Nullable List<ProduceRequestHeader> headers,
        @JsonProperty("key") @Nullable ProduceRequestData key,
        @JsonProperty("value") @Nullable ProduceRequestData value,
        @JsonProperty("timestamp") @Nullable Instant timestamp) {
      return ProduceBatchRequestEntry.builder()
          .setId(id)
          .setPartitionId(partitionId)
          .setHeaders(headers != null ? headers : ImmutableList.of())
          .setKey(key)
          .setValue(value)
          .setTimestamp(timestamp);
    }

    public abstract Builder setId(JsonNode id);

    public abstract Builder setPartitionId(@Nullable Integer partitionId);

    public abstract Builder setHeaders(List<ProduceRequestHeader> headers);

    public abstract Builder setKey(@Nullable ProduceRequestData key);

    public abstract Builder setValue(@Nullable ProduceRequestData value);

    public abstract Builder setTimestamp(@Nullable Instant timestamp);

    public abstract Builder setOriginalSize(long size);

    abstract ProduceBatchRequestEntry autoBuild();

    public final ProduceBatchRequestEntry build() {
      ProduceBatchRequestEntry entry = autoBuild();

      // Even though the OpenAPI specification requires a string for the entry ids of the batch
      // operations, Jackson coerces non-strings into strings. We demand proper quoting so that
      // entry ids in responses are textually identical to requests.

      JsonNode idNode = entry.getId();

      // To check for null and also non-strings coerced into strings, we check the JsonNode type
      if (idNode.getNodeType() != JsonNodeType.STRING) {
        throw Errors.produceBatchException(
            Errors.PRODUCE_BATCH_EXCEPTION_IDENTIFIER_NOT_VALID_MESSAGE);
      }

      String idString = idNode.asText();
      if (idString.length() < ProduceBatchAction.BATCH_ID_MINIMUM_LENGTH
          || idString.length() > ProduceBatchAction.BATCH_ID_MAXIMUM_LENGTH) {
        throw Errors.produceBatchException(
            Errors.PRODUCE_BATCH_EXCEPTION_IDENTIFIER_NOT_VALID_MESSAGE);
      }

      if (!idString.matches("[0-9a-zA-Z-_]+")) {
        throw Errors.produceBatchException(
            Errors.PRODUCE_BATCH_EXCEPTION_IDENTIFIER_NOT_VALID_MESSAGE);
      }

      return entry;
    }
  }

  static final class Deserializer extends JsonDeserializer<ProduceBatchRequestEntry> {

    @Override
    public ProduceBatchRequestEntry deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      long start =
          parser.getCurrentLocation().getByteOffset() == -1
              ? parser.getCurrentLocation().getCharOffset()
              : parser.getCurrentLocation().getByteOffset();
      ProduceBatchRequestEntry.Builder builder =
          parser.readValueAs(ProduceBatchRequestEntry.Builder.class);
      long end =
          parser.getCurrentLocation().getByteOffset() == -1
              ? parser.getCurrentLocation().getCharOffset()
              : parser.getCurrentLocation().getByteOffset();
      long size = start == -1 || end == -1 ? 0 : end - start + 1;
      builder.setOriginalSize(size);
      return builder.build();
    }
  }
}
