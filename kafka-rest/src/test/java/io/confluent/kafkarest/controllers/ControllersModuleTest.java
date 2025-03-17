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

package io.confluent.kafkarest.controllers;

import static org.junit.jupiter.api.Assertions.assertSame;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.controllers.ControllersModule.SchemaRecordSerializerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
class ControllersModuleTest extends EasyMockSupport {

  @Test
  void testSchemaRecordSerializerSingleton() {
    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    replayAll();

    Map<String, Object> commonConfig = new HashMap<>();
    commonConfig.put("schema.registry.url", "http://dummy-schema-registry-url");

    SchemaRecordSerializerFactory schemaRecordSerializerFactory =
        new SchemaRecordSerializerFactory(
            Optional.ofNullable(mockSchemaRegistryClient),
            commonConfig,
            commonConfig,
            commonConfig);

    SchemaRecordSerializer serializerInstanceOne = schemaRecordSerializerFactory.provide();
    SchemaRecordSerializer serializerInstanceTwo = schemaRecordSerializerFactory.provide();

    assertSame(
        serializerInstanceOne, serializerInstanceTwo, "Expected both instances to be the same");
    verifyAll();
  }
}
