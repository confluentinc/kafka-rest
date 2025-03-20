package io.confluent.kafkarest.controllers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.config.ConfigModule.AvroSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.JsonschemaSerializerConfigs;
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
  private static final class AvroSerializerConfigsImpl
      extends AnnotationLiteral<AvroSerializerConfigs> implements AvroSerializerConfigs {}

  private static final class JsonschemaSerializerConfigsImpl
      extends AnnotationLiteral<JsonschemaSerializerConfigs>
      implements JsonschemaSerializerConfigs {}

  private static final class ProtobufSerializerConfigsImpl
      extends AnnotationLiteral<ProtobufSerializerConfigs> implements ProtobufSerializerConfigs {}

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
                .qualifiedBy(new AvroSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});
            bind(dummyConfig)
                .qualifiedBy(new JsonschemaSerializerConfigsImpl())
                .to(new TypeLiteral<Map<String, Object>>() {});
            bind(dummyConfig)
                .qualifiedBy(new ProtobufSerializerConfigsImpl())
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
