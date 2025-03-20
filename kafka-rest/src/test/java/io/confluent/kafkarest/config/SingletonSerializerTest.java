/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.controllers.ControllersModule;
import io.confluent.kafkarest.controllers.SchemaRecordSerializer;
import io.confluent.kafkarest.controllers.SchemaRecordSerializerThrowing;
import java.util.HashMap;
import java.util.Map;
import org.easymock.EasyMock;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SingletonSerializerTest {

  private ServiceLocator serviceLocator;

  @BeforeEach
  public void setUp() {
    serviceLocator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    SchemaRegistryClient mockSchemaRegistryClient = EasyMock.createMock(SchemaRegistryClient.class);
    EasyMock.replay(mockSchemaRegistryClient);
    ServiceLocatorUtilities.bind(
        serviceLocator,
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(mockSchemaRegistryClient).to(SchemaRegistryClient.class);
            Map<String, Object> dummyConfig = new HashMap<>();
            dummyConfig.put("schema.registry.url", "http://localhost:8081");

            bind(dummyConfig)
                .qualifiedBy(new ConfigModule.AvroSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});
            bind(dummyConfig)
                .qualifiedBy(new ConfigModule.JsonschemaSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});
            bind(dummyConfig)
                .qualifiedBy(new ConfigModule.ProtobufSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});

            install(new ControllersModule());
          }
        });
    EasyMock.verify(mockSchemaRegistryClient);
  }

  @Test
  public void testSchemaRecordSerializerSingleton() {
    SchemaRecordSerializer firstInstance = serviceLocator.getService(SchemaRecordSerializer.class);
    SchemaRecordSerializer secondInstance = serviceLocator.getService(SchemaRecordSerializer.class);

    assertSame(
        firstInstance, secondInstance, "Instances should be the same due to singleton scope");

    assertFalse(
        firstInstance instanceof SchemaRecordSerializerThrowing,
        "Instance should not be of SchemaRecordSerializerThrowing");
    assertFalse(
        secondInstance instanceof SchemaRecordSerializerThrowing,
        "Instance should not be of SchemaRecordSerializerThrowing");
  }
}
