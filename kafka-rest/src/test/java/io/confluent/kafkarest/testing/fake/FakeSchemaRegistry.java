/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafkarest.testing.AbstractSchemaRegistry;

public final class FakeSchemaRegistry extends AbstractSchemaRegistry {
  private final MockSchemaRegistryClient schemaRegistry =
      new MockSchemaRegistryClient(
          ImmutableList.of(
              new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()));

  @Override
  public MockSchemaRegistryClient getClient() {
    return schemaRegistry;
  }
}
