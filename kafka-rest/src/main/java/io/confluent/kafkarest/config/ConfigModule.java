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

import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.RestConfig;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Set;
import javax.inject.Qualifier;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * A module to populate the injector with the configuration passed to this application.
 *
 * <p>In addition to {@link KafkaRestConfig}, which contains all configuration, individual
 * configuration properties are also exposed, on a need-to-know basis.</p>
 */
// CHECKSTYLE:OFF:ClassDataAbstractionCoupling
public final class ConfigModule extends AbstractBinder {

  private final KafkaRestConfig config;
  private final SchemaRegistryConfig schemaRegistryConfig;

  public ConfigModule(KafkaRestConfig config) {
    this.config = config;
    schemaRegistryConfig = new SchemaRegistryConfig(config.getSchemaRegistryConfigs());
  }

  @Override
  protected void configure() {
    bind(config).to(KafkaRestConfig.class);

    // Keep this list alphabetically sorted.
    bind(
        config.getList(KafkaRestConfig.ADVERTISED_LISTENERS_CONFIG).stream()
            .map(URI::create)
            .collect(Collectors.toList()))
        .qualifiedBy(new AdvertisedListenersConfigImpl())
        .to(new TypeLiteral<List<URI>>() { });

    bind(new HashSet<>(config.getList(KafkaRestConfig.API_ENDPOINTS_BLOCKLIST_CONFIG)))
        .qualifiedBy(new ApiEndpointsBlocklistConfigImpl())
        .to(new TypeLiteral<Set<String>>() { });

    bind(config.getAvroSerializerConfigs())
        .qualifiedBy(new AvroSerializerConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(config.getString(KafkaRestConfig.CRN_AUTHORITY_CONFIG))
        .qualifiedBy(new CrnAuthorityConfigImpl())
        .to(String.class);

    bind(config.getString(KafkaRestConfig.HOST_NAME_CONFIG))
        .qualifiedBy(new HostNameConfigImpl())
        .to(String.class);

    bind(config.getJsonSerializerConfigs())
        .qualifiedBy(new JsonSerializerConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(config.getJsonschemaSerializerConfigs())
        .qualifiedBy(new JsonschemaSerializerConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(
        config.getList(RestConfig.LISTENERS_CONFIG).stream()
            .map(URI::create)
            .collect(Collectors.toList()))
        .qualifiedBy(new ListenersConfigImpl())
        .to(new TypeLiteral<List<URI>>() { });

    bind(schemaRegistryConfig.getMaxSchemasPerSubject())
        .qualifiedBy(new MaxSchemasPerSubjectConfigImpl())
        .to(Integer.class);

    bind(config.getInt(RestConfig.PORT_CONFIG))
        .qualifiedBy(new PortConfigImpl())
        .to(Integer.class);

    bind(config.getProducerConfigs())
        .qualifiedBy(new ProducerConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(config.getProtobufSerializerConfigs())
        .qualifiedBy(new ProtobufSerializerConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(config.getSchemaRegistryConfigs())
        .qualifiedBy(new SchemaRegistryConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(schemaRegistryConfig.requestHeaders())
        .qualifiedBy(new SchemaRegistryRequestHeadersConfigImpl())
        .to(new TypeLiteral<Map<String, String>>() { });

    bind(
        schemaRegistryConfig.getSchemaRegistryUrls()
            .stream()
            .map(URI::create)
            .collect(Collectors.toList()))
        .qualifiedBy(new SchemaRegistryUrlsConfigImpl())
        .to(new TypeLiteral<List<URI>>() { });

    bind(schemaRegistryConfig.getSubjectNameStrategy()).to(SubjectNameStrategy.class);
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface AdvertisedListenersConfig {
  }

  private static final class AdvertisedListenersConfigImpl
      extends AnnotationLiteral<AdvertisedListenersConfig> implements AdvertisedListenersConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ApiEndpointsBlocklistConfig {
  }

  private static final class ApiEndpointsBlocklistConfigImpl
      extends AnnotationLiteral<ApiEndpointsBlocklistConfig>
      implements ApiEndpointsBlocklistConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface AvroSerializerConfigs {
  }

  private static final class AvroSerializerConfigsImpl
      extends AnnotationLiteral<AvroSerializerConfigs> implements AvroSerializerConfigs {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface CrnAuthorityConfig {
  }

  private static final class CrnAuthorityConfigImpl
      extends AnnotationLiteral<CrnAuthorityConfig> implements CrnAuthorityConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  @Deprecated
  public @interface HostNameConfig {
  }

  private static final class HostNameConfigImpl
      extends AnnotationLiteral<HostNameConfig> implements HostNameConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface JsonSerializerConfigs {
  }

  private static final class JsonSerializerConfigsImpl
      extends AnnotationLiteral<JsonSerializerConfigs> implements JsonSerializerConfigs {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface JsonschemaSerializerConfigs {
  }

  private static final class JsonschemaSerializerConfigsImpl
      extends AnnotationLiteral<JsonschemaSerializerConfigs>
      implements JsonschemaSerializerConfigs {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface ListenersConfig {
  }

  private static final class ListenersConfigImpl
      extends AnnotationLiteral<ListenersConfig> implements ListenersConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface MaxSchemasPerSubjectConfig {
  }

  private static final class MaxSchemasPerSubjectConfigImpl
      extends AnnotationLiteral<MaxSchemasPerSubjectConfig> implements MaxSchemasPerSubjectConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  @Deprecated
  public @interface PortConfig {
  }

  private static final class PortConfigImpl
      extends AnnotationLiteral<PortConfig> implements PortConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface ProducerConfigs {
  }

  private static final class ProducerConfigsImpl
      extends AnnotationLiteral<ProducerConfigs> implements ProducerConfigs {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProtobufSerializerConfigs {
  }

  private static final class ProtobufSerializerConfigsImpl
      extends AnnotationLiteral<ProtobufSerializerConfigs> implements ProtobufSerializerConfigs {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface SchemaRegistryConfigs {
  }

  private static final class SchemaRegistryConfigsImpl
      extends AnnotationLiteral<SchemaRegistryConfigs> implements SchemaRegistryConfigs {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface SchemaRegistryRequestHeadersConfig {
  }

  private static final class SchemaRegistryRequestHeadersConfigImpl
      extends AnnotationLiteral<SchemaRegistryRequestHeadersConfig>
      implements SchemaRegistryRequestHeadersConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface SchemaRegistryUrlsConfig {
  }

  private static final class SchemaRegistryUrlsConfigImpl
      extends AnnotationLiteral<SchemaRegistryUrlsConfig> implements SchemaRegistryUrlsConfig {
  }
}
// CHECKSTYLE:ON:ClassDataAbstractionCoupling
