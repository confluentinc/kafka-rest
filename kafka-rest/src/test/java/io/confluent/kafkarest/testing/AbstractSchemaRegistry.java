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

package io.confluent.kafkarest.testing;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

public abstract class AbstractSchemaRegistry {

  public abstract SchemaRegistryClient getClient();

  public final SchemaKey createSchema(String subject, ParsedSchema schema) throws Exception {
    int schemaId = getClient().register(subject, schema);
    int schemaVersion = getClient().getVersion(subject, schema);
    return SchemaKey.create(subject, schemaId, schemaVersion);
  }

  public final KafkaAvroDeserializer createAvroDeserializer() {
    return new KafkaAvroDeserializer(getClient());
  }

  public final KafkaJsonSchemaDeserializer<Object> createJsonSchemaDeserializer() {
    return new KafkaJsonSchemaDeserializer<>(getClient());
  }

  public final KafkaProtobufDeserializer<Message> createProtobufDeserializer() {
    return new KafkaProtobufDeserializer<>(getClient());
  }

  @AutoValue
  public abstract static class SchemaKey {

    SchemaKey() {}

    public abstract String getSubject();

    public abstract int getSchemaId();

    public abstract int getSchemaVersion();

    public static SchemaKey create(String subject, int schemaId, int schemaVersion) {
      return new AutoValue_AbstractSchemaRegistry_SchemaKey(subject, schemaId, schemaVersion);
    }
  }
}
