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

import io.confluent.kafkarest.extension.ContextInvocationHandler;
import io.confluent.kafkarest.extension.InstantConverterProvider;
import io.confluent.kafkarest.extension.KafkaRestCleanupFilter;
import io.confluent.kafkarest.extension.KafkaRestContextProvider;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.resources.BrokersResource;
import io.confluent.kafkarest.resources.ConsumersResource;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.resources.ConsumerGroupsResource;
import io.confluent.kafkarest.resources.RootResource;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.KafkaExceptionMapper;
import io.confluent.rest.exceptions.WebApplicationExceptionMapper;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.Configurable;
import org.eclipse.jetty.util.StringUtil;


/**
 * Utilities for configuring and running an embedded Kafka server.
 */
public class KafkaRestApplication extends Application<KafkaRestConfig> {

  List<RestResourceExtension> restResourceExtensions;

  public KafkaRestApplication() throws RestConfigException {
    this(new Properties());
  }

  public KafkaRestApplication(Properties props) throws RestConfigException {
    super(new KafkaRestConfig(props));
  }

  public KafkaRestApplication(KafkaRestConfig config) {
    super(config);

    restResourceExtensions = config.getConfiguredInstances(
        KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG,
        RestResourceExtension.class);
  }


  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    setupInjectedResources(config, appConfig, null,
        null, null, null, null
    );
  }

  /**
   * Helper that does normal setup, but uses injected components so their configs or implementations
   * can be customized for testing. This only exists to support TestKafkaRestApplication
   */
  protected void setupInjectedResources(
      Configurable<?> config, KafkaRestConfig appConfig,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager,
      AdminClientWrapper adminClientWrapperInjected,
      GroupMetadataObserver groupMetadataObserver,
      ScalaConsumersContext scalaConsumersContext
  ) {
    if (StringUtil.isBlank(appConfig.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG))
        && StringUtil.isBlank(appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG))) {
      throw new RuntimeException("Atleast one of " + KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG + " "
                                 + "or "
                                    + KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG
                                    + " needs to be configured");
    }

    KafkaRestContextProvider.initialize(config, appConfig, producerPool,
        kafkaConsumerManager, adminClientWrapperInjected,
        groupMetadataObserver, scalaConsumersContext);
    ContextInvocationHandler contextInvocationHandler = new ContextInvocationHandler();
    KafkaRestContext context =
        (KafkaRestContext) Proxy.newProxyInstance(
            KafkaRestContext.class.getClassLoader(),
            new Class[]{KafkaRestContext.class},
            contextInvocationHandler
        );
    config.register(RootResource.class);
    config.register(new BrokersResource(context));
    config.register(new TopicsResource(context));
    config.register(new PartitionsResource(context));
    config.register(new ConsumersResource(context));
    config.register(new ConsumerGroupsResource(context));
    config.register(new io.confluent.kafkarest.resources.v2.ConsumersResource(context));
    config.register(new io.confluent.kafkarest.resources.v2.PartitionsResource(context));
    config.register(KafkaRestCleanupFilter.class);
    config.register(InstantConverterProvider.class);

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.register(config, appConfig);
    }
  }

  @Override
  protected void registerExceptionMappers(Configurable<?> config, KafkaRestConfig restConfig) {
    config.register(ConstraintViolationExceptionMapper.class);
    config.register(new WebApplicationExceptionMapper(restConfig));
    config.register(new KafkaExceptionMapper(restConfig));
  }

  @Override
  public void onShutdown() {

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.clean();
    }

    KafkaRestContextProvider.clean();
  }
}
