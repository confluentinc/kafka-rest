/*
 * Copyright 2025 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.config.ConfigModule.AvroSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.JsonschemaSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.NullRequestBodyAlwaysPublishEmptyRecordEnabledConfig;
import io.confluent.kafkarest.config.ConfigModule.ProtobufSerializerConfigs;
import java.util.HashMap;
import java.util.Map;
import org.easymock.EasyMock;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ControllersModuleTest {

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
                .qualifiedBy(new TestAvroSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});
            bind(dummyConfig)
                .qualifiedBy(new TestJsonschemaSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});
            bind(dummyConfig)
                .qualifiedBy(new TestProtobufSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});

            bind(false)
                .qualifiedBy(new TestNullRequestBodyAlwaysPublishEmptyRecordEnabledConfigImpl())
                .to(Boolean.class);
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

  private static final class TestAvroSerializerConfigsImpl
      extends AnnotationLiteral<AvroSerializerConfigs> implements AvroSerializerConfigs {}

  private static final class TestJsonschemaSerializerConfigsImpl
      extends AnnotationLiteral<JsonschemaSerializerConfigs>
      implements JsonschemaSerializerConfigs {}

  private static final class TestProtobufSerializerConfigsImpl
      extends AnnotationLiteral<ProtobufSerializerConfigs> implements ProtobufSerializerConfigs {}

  private static final class TestNullRequestBodyAlwaysPublishEmptyRecordEnabledConfigImpl
      extends AnnotationLiteral<NullRequestBodyAlwaysPublishEmptyRecordEnabledConfig>
      implements NullRequestBodyAlwaysPublishEmptyRecordEnabledConfig {}
}
