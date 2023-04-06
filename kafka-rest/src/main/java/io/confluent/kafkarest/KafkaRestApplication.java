/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import io.confluent.kafkarest.backends.BackendsModule;
import io.confluent.kafkarest.config.ConfigModule;
import io.confluent.kafkarest.controllers.ControllersModule;
import io.confluent.kafkarest.exceptions.ExceptionsModule;
import io.confluent.kafkarest.exceptions.KafkaRestExceptionMapper;
import io.confluent.kafkarest.extension.EnumConverterProvider;
import io.confluent.kafkarest.extension.ContextInvocationHandler;
import io.confluent.kafkarest.extension.InstantConverterProvider;
import io.confluent.kafkarest.extension.KafkaRestCleanupFilter;
import io.confluent.kafkarest.extension.KafkaRestContextProvider;
import io.confluent.kafkarest.extension.ResourceBlocklistFeature;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.resources.ResourcesFeature;
import io.confluent.kafkarest.response.ResponseModule;
import io.confluent.rest.Application;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.WebApplicationExceptionMapper;
import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import javax.ws.rs.core.Configurable;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.StringUtil;
import org.glassfish.jersey.server.ServerProperties;

/**
 * Utilities for configuring and running an embedded Kafka server.
 */
public class KafkaRestApplication extends Application<KafkaRestConfig> {

  List<RestResourceExtension> restResourceExtensions;

  public KafkaRestApplication() {
    this(new Properties());
  }

  public KafkaRestApplication(Properties props) {
    this(new KafkaRestConfig(props));
  }

  public KafkaRestApplication(KafkaRestConfig config) {
    this(config, /* path= */ "");
  }

  public KafkaRestApplication(KafkaRestConfig config, String path) {
    super(config, path);

    restResourceExtensions = config.getConfiguredInstances(
        KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG,
        RestResourceExtension.class);
  }

  @Override
  public void configurePreResourceHandling(ServletContextHandler context) {
  }

  @Override
  public void configurePostResourceHandling(ServletContextHandler context) {
  }

  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    if (StringUtil.isBlank(appConfig.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG))
        && StringUtil.isBlank(appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG))) {
      throw new RuntimeException("Atleast one of " + KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG + " "
                                 + "or "
                                    + KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG
                                    + " needs to be configured");
    }
    KafkaRestContextProvider.initialize(appConfig);
    ContextInvocationHandler contextInvocationHandler = new ContextInvocationHandler();
    KafkaRestContext context =
        (KafkaRestContext) Proxy.newProxyInstance(
            KafkaRestContext.class.getClassLoader(),
            new Class[]{KafkaRestContext.class},
            contextInvocationHandler
        );

    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    config.register(new BackendsModule());
    config.register(new ConfigModule(appConfig));
    config.register(new ControllersModule());
    config.register(new ExceptionsModule());
    config.register(new ResourcesFeature(context, appConfig));
    config.register(new ResponseModule());

    config.register(ResourceBlocklistFeature.class);
    config.register(KafkaRestCleanupFilter.class);

    config.register(EnumConverterProvider.class);
    config.register(InstantConverterProvider.class);

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.register(config, appConfig);
    }
  }

  @Override
  public ObjectMapper getJsonMapper() {
    return super.getJsonMapper()
        .registerModule(new GuavaModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Override
  protected void registerExceptionMappers(Configurable<?> config, KafkaRestConfig restConfig) {
    config.register(JsonParseExceptionMapper.class);
    config.register(JsonMappingExceptionMapper.class);
    config.register(ConstraintViolationExceptionMapper.class);
    config.register(new WebApplicationExceptionMapper(restConfig));
    config.register(new KafkaRestExceptionMapper(restConfig));
  }

  @Override
  public void onShutdown() {

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.clean();
    }

    KafkaRestContextProvider.clean();
  }
}
