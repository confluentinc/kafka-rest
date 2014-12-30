/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest;

import io.confluent.kafkarest.resources.*;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;

import javax.ws.rs.core.Configurable;
import java.util.Properties;

/**
 * Utilities for configuring and running an embedded Kafka server.
 */
public class KafkaRestApplication extends Application<KafkaRestConfig> {
    Context context;

    public KafkaRestApplication() throws RestConfigException {
        this(new Properties());
    }

    public KafkaRestApplication(Properties props) throws RestConfigException {
        this(new KafkaRestConfig(props));
    }

    public KafkaRestApplication(KafkaRestConfig config) {
        this.config = config;
    }

    @Override
    public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
        MetadataObserver mdObserver = new MetadataObserver(appConfig);
        ProducerPool producerPool = new ProducerPool(appConfig);
        ConsumerManager consumerManager = new ConsumerManager(appConfig, mdObserver);
        context = new Context(appConfig, mdObserver, producerPool, consumerManager);
        config.register(RootResource.class);
        config.register(new BrokersResource(context));
        config.register(new TopicsResource(context));
        config.register(PartitionsResource.class);
        config.register(new ConsumersResource(context));
    }

    @Override
    public KafkaRestConfig configure() throws RestConfigException {
        return config;
    }

    @Override
    public void onShutdown() {
        context.getConsumerManager().shutdown();
        context.getProducerPool().shutdown();
        context.getMetadataObserver().shutdown();
    }
}
