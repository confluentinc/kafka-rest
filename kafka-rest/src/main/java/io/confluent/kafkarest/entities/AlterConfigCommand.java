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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;

public abstract class AlterConfigCommand {

  private AlterConfigCommand() {
  }

  public static AlterConfigCommand set(String name, @Nullable String value) {
    return UpdateConfigCommand.create(name, value);
  }

  public static AlterConfigCommand delete(String name) {
    return DeleteConfigCommand.create(name);
  }

  public abstract String getName();

  public abstract AlterConfigOp toAlterConfigOp();

  @AutoValue
  abstract static class UpdateConfigCommand extends AlterConfigCommand {

    UpdateConfigCommand() {
    }

    abstract Optional<String> getValue();

    private static UpdateConfigCommand create(String name, @Nullable String value) {
      return new AutoValue_AlterConfigCommand_UpdateConfigCommand(name, Optional.ofNullable(value));
    }

    @Override
    public final AlterConfigOp toAlterConfigOp() {
      return new AlterConfigOp(
          new ConfigEntry(getName(), getValue().orElse(null)), AlterConfigOp.OpType.SET);
    }
  }

  @AutoValue
  abstract static class DeleteConfigCommand extends AlterConfigCommand {

    DeleteConfigCommand() {
    }

    private static DeleteConfigCommand create(String name) {
      return new AutoValue_AlterConfigCommand_DeleteConfigCommand(name);
    }

    @Override
    public final AlterConfigOp toAlterConfigOp() {
      return new AlterConfigOp(
          new ConfigEntry(getName(), /* value= */ null), AlterConfigOp.OpType.DELETE);
    }
  }
}
