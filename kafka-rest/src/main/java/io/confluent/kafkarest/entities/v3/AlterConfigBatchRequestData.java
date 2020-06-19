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

import static java.util.Collections.unmodifiableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class AlterConfigBatchRequestData {

  AlterConfigBatchRequestData() {
  }

  @JsonProperty("data")
  public abstract ImmutableList<AlterEntry> getData();

  public static AlterConfigBatchRequestData create(List<AlterEntry> data) {
    return new AutoValue_AlterConfigBatchRequestData(ImmutableList.copyOf(data));
  }

  @JsonCreator
  static AlterConfigBatchRequestData fromJson(@JsonProperty("data") List<AlterEntry> data) {
    return create(data);
  }

  public final List<AlterConfigCommand> toAlterConfigCommands() {
    ArrayList<AlterConfigCommand> commands = new ArrayList<>();
    for (AlterEntry entry : getData()) {
      switch (entry.getOperation()) {
        case SET:
          commands.add(AlterConfigCommand.set(entry.getName(), entry.getValue().orElse(null)));
          break;

        case DELETE:
          commands.add(AlterConfigCommand.delete(entry.getName()));
          break;

        default:
          throw new AssertionError("unreachable");
      }
    }
    return unmodifiableList(commands);
  }

  @AutoValue
  public abstract static class AlterEntry {

    @JsonProperty("name")
    public abstract String getName();

    @JsonProperty("value")
    public abstract Optional<String> getValue();

    @JsonProperty("operation")
    public abstract AlterOperation getOperation();

    public static Builder builder() {
      return new AutoValue_AlterConfigBatchRequestData_AlterEntry.Builder()
          .setOperation(AlterOperation.SET);
    }

    @JsonCreator
    static AlterEntry fromJson(
        @JsonProperty("name") String name,
        @JsonProperty("value") @Nullable String value,
        @JsonProperty("operation") @Nullable AlterOperation operation
    ) {
      return builder()
          .setName(name)
          .setValue(value)
          .setOperation(operation != null ? operation : AlterOperation.SET)
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setName(String name);

      public abstract Builder setValue(@Nullable String value);

      public abstract Builder setOperation(AlterOperation operation);

      public abstract AlterEntry build();
    }
  }

  public enum AlterOperation {

    SET,

    DELETE
  }
}
