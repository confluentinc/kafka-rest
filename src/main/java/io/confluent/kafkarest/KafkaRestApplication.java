/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest;

import io.confluent.kafkarest.exceptions.ZkExceptionMapper;
import io.confluent.kafkarest.extension.ContextInvocationHandler;
import io.confluent.kafkarest.extension.KafkaRestCleanupFilter;
import io.confluent.kafkarest.extension.KafkaRestContextProvider;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.resources.BrokersResource;
import io.confluent.kafkarest.resources.RootResource;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.kafkarest.resources.XHeaderReflectingResponseFilter;
import io.confluent.kafkarest.resources.v2.ConsumersResource;
import io.confluent.kafkarest.resources.v2.PartitionsResource;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;

import java.lang.reflect.Proxy;
import java.util.Properties;
import javax.ws.rs.core.Configurable;

import kafka.utils.ZkUtils;
import org.eclipse.jetty.util.StringUtil;


/**
 * Utilities for configuring and running an embedded Kafka server.
 */
public class KafkaRestApplication extends Application<KafkaRestConfig> {

  RestResourceExtension restResourceExtension;

  public KafkaRestApplication() throws RestConfigException {
    this(new Properties());
  }

  public KafkaRestApplication(Properties props) throws RestConfigException {
    super(new KafkaRestConfig(props));
  }

  /**
   * Configure the application.
   */
  public KafkaRestApplication(KafkaRestConfig config)
      throws IllegalAccessException, InstantiationException, RestConfigException {
    super(config);

    String extensionClassName =
        config.getString(KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG);

    if (StringUtil.isNotBlank(extensionClassName)) {
      try {
        Class<RestResourceExtension>
            restResourceExtensionClass =
            (Class<RestResourceExtension>) Class.forName(extensionClassName);

        restResourceExtension = restResourceExtensionClass.newInstance();
      } catch (ClassNotFoundException e) {
        throw new RestConfigException(
            "Unable to load resource extension class " + extensionClassName
            + ". Check your classpath and that the configured class implements "
            + "the RestResourceExtension interface.");
      }
    }
  }


  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    setupInjectedResources(config, appConfig, null, null, null, null, null, null, null,
        null
    );
  }

  /**
   * Helper that does normal setup, but uses injected components so their configs or implementations
   * can be customized for testing. This only exists to support TestKafkaRestApplication
   */
  protected void setupInjectedResources(
      Configurable<?> config, KafkaRestConfig appConfig,
      ZkUtils zkUtils, MetadataObserver mdObserver,
      ProducerPool producerPool,
      ConsumerManager consumerManager,
      SimpleConsumerFactory simpleConsumerFactory,
      SimpleConsumerManager simpleConsumerManager,
      KafkaConsumerManager kafkaConsumerManager,
      AdminClientWrapper adminClientWrapperInjected
  ) {
    if (StringUtil.isBlank(appConfig.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG))
        && StringUtil.isBlank(appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG))) {
      throw new RuntimeException("Atleast one of " + KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG + " "
                                 + "or "
                                    + KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG
                                    + " needs to be configured");
    }

    config.register(new ZkExceptionMapper(appConfig));

    KafkaRestContextProvider.initialize(zkUtils, appConfig, mdObserver, producerPool,
        consumerManager, simpleConsumerFactory,
        simpleConsumerManager, kafkaConsumerManager, adminClientWrapperInjected
    );
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
    config.register(new io.confluent.kafkarest.resources.v2.ConsumersResource(context));
    config.register(new io.confluent.kafkarest.resources.v2.PartitionsResource(context));
    config.register(KafkaRestCleanupFilter.class);
    config.register(new XHeaderReflectingResponseFilter(appConfig));

    if (restResourceExtension != null) {
      restResourceExtension.register(config, appConfig);
    }
  }

  @Override
  public void onShutdown() {
    if (restResourceExtension != null) {
      restResourceExtension.clean();
    }
    KafkaRestContextProvider.clean();
  }
}
