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

import static java.util.Objects.requireNonNull;

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
import javax.inject.Inject;
import javax.inject.Qualifier;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.Factory;
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

  public ConfigModule(KafkaRestConfig config) {
    this.config = requireNonNull(config);
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

    bind(config.getString(KafkaRestConfig.CRN_AUTHORITY_CONFIG))
        .qualifiedBy(new CrnAuthorityConfigImpl())
        .to(String.class);

    bind(config.getString(KafkaRestConfig.HOST_NAME_CONFIG))
        .qualifiedBy(new HostNameConfigImpl())
        .to(String.class);

    bind(
        config.getList(RestConfig.LISTENERS_CONFIG).stream()
            .map(URI::create)
            .collect(Collectors.toList()))
        .qualifiedBy(new ListenersConfigImpl())
        .to(new TypeLiteral<List<URI>>() { });

    bindFactory(MaxSchemasPerSubjectFactory.class)
        .qualifiedBy(new MaxSchemasPerSubjectConfigImpl())
        .to(Integer.class);

    bind(config.getInt(RestConfig.PORT_CONFIG))
        .qualifiedBy(new PortConfigImpl())
        .to(Integer.class);

    bindFactory(SchemaRegistryConfigFactory.class).to(SchemaRegistryConfig.class);

    bind(config.getProducerConfigs())
        .qualifiedBy(new ProducerConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bind(config.getSchemaRegistryConfigs())
        .qualifiedBy(new SchemaRegistryConfigsImpl())
        .to(new TypeLiteral<Map<String, Object>>() { });

    bindFactory(SchemaRegistryRequestHeadersFactory.class)
        .qualifiedBy(new SchemaRegistryRequestHeadersConfigImpl())
        .to(new TypeLiteral<Map<String, String>>() { });

    bindFactory(SchemaRegistryUrlsFactory.class)
        .qualifiedBy(new SchemaRegistryUrlsConfigImpl())
        .to(new TypeLiteral<List<URI>>() { });

    bindFactory(SubjectNameStrategyFactory.class).to(SubjectNameStrategy.class);
  }

  private static final class MaxSchemasPerSubjectFactory implements Factory<Integer> {
    private final SchemaRegistryConfig config;

    @Inject
    private MaxSchemasPerSubjectFactory(SchemaRegistryConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public Integer provide() {
      return config.getMaxSchemasPerSubject();
    }

    @Override
    public void dispose(Integer unused) {
    }
  }

  private static final class SchemaRegistryConfigFactory implements Factory<SchemaRegistryConfig> {
    private final Map<String, Object> configs;

    @Inject
    private SchemaRegistryConfigFactory(@SchemaRegistryConfigs Map<String, Object> configs) {
      this.configs = requireNonNull(configs);
    }

    @Override
    public SchemaRegistryConfig provide() {
      return new SchemaRegistryConfig(configs);
    }

    @Override
    public void dispose(SchemaRegistryConfig unused) {
    }
  }

  private static final class SchemaRegistryRequestHeadersFactory
      implements Factory<Map<String, String>> {
    private final SchemaRegistryConfig config;

    @Inject
    private SchemaRegistryRequestHeadersFactory(SchemaRegistryConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public Map<String, String> provide() {
      return config.requestHeaders();
    }

    @Override
    public void dispose(Map<String, String> unused) {
    }
  }

  private static final class SchemaRegistryUrlsFactory implements Factory<List<URI>> {
    private final SchemaRegistryConfig config;

    @Inject
    private SchemaRegistryUrlsFactory(SchemaRegistryConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public List<URI> provide() {
      return config.getSchemaRegistryUrls().stream()
          .map(URI::create)
          .collect(Collectors.toList());
    }

    @Override
    public void dispose(List<URI> unused) {
    }
  }

  private static final class SubjectNameStrategyFactory implements Factory<SubjectNameStrategy> {
    private final SchemaRegistryConfig config;

    @Inject
    private SubjectNameStrategyFactory(SchemaRegistryConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public SubjectNameStrategy provide() {
      return config.getSubjectNameStrategy();
    }

    @Override
    public void dispose(SubjectNameStrategy unused) {
    }
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
