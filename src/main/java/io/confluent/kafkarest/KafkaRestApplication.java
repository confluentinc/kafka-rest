/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest;

import java.util.Properties;

import javax.ws.rs.core.Configurable;

import io.confluent.kafkarest.extension.ContextProviderFactoryBinder;
import io.confluent.kafkarest.extension.DefaultContextProvider;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.kafkarest.exceptions.ZkExceptionMapper;
import io.confluent.kafkarest.resources.BrokersResource;
import io.confluent.kafkarest.resources.ConsumersResource;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.resources.RootResource;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import kafka.utils.ZkUtils;


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

  public KafkaRestApplication(KafkaRestConfig config) {
    super(config);
  }

  public KafkaRestApplication(KafkaRestConfig config, RestResourceExtension restResourceExtension)
      throws IllegalAccessException, InstantiationException {
    super(config);
    this.restResourceExtension = restResourceExtension;
  }

  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    setupInjectedResources(config, appConfig, null, null, null, null, null, null, null);
  }

  /**
   * Helper that does normal setup, but uses injected components so their configs or implementations
   * can be customized for testing. This only exists to support TestKafkaRestApplication
   */
  protected void setupInjectedResources(Configurable<?> config, KafkaRestConfig appConfig,
                                        ZkUtils zkUtils, MetadataObserver mdObserver,
                                        ProducerPool producerPool,
                                        ConsumerManager consumerManager,
                                        SimpleConsumerFactory simpleConsumerFactory,
                                        SimpleConsumerManager simpleConsumerManager,
                                        KafkaConsumerManager kafkaConsumerManager) {

    config.register(new ZkExceptionMapper(appConfig));

    DefaultContextProvider.initializeDefaultContext(zkUtils, appConfig, mdObserver, producerPool,
                                                    consumerManager, simpleConsumerFactory,
                                                    simpleConsumerManager, kafkaConsumerManager);
    config.register(RootResource.class);
    config.register(BrokersResource.class);
    config.register(TopicsResource.class);
    config.register(PartitionsResource.class);
    config.register(ConsumersResource.class);
    config.register(io.confluent.kafkarest.resources.v2.ConsumersResource.class);
    config.register(io.confluent.kafkarest.resources.v2.PartitionsResource.class);
    config.register(new ContextProviderFactoryBinder());

    if (restResourceExtension != null) {
      restResourceExtension.register(config, appConfig);
    }
  }

  @Override
  public void onShutdown() {
    if (restResourceExtension != null) {
      restResourceExtension.clean();
    }
    DefaultContextProvider.clean();

  }
}
