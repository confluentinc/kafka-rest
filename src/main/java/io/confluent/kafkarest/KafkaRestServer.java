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

import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import io.confluent.kafkarest.exceptions.GenericExceptionMapper;
import io.confluent.kafkarest.exceptions.WebApplicationExceptionMapper;
import io.confluent.kafkarest.resources.*;
import io.confluent.kafkarest.validation.ConstraintViolationExceptionMapper;
import io.confluent.kafkarest.validation.JacksonMessageBodyProvider;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.validation.ValidationFeature;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.core.Configurable;
import java.util.Properties;

/**
 * Utilities for configuring and running an embedded Kafka server.
 */
public class KafkaRestServer {
    /**
     * Configure and create the server.
     */
    public static Server createServer(Properties props) throws ConfigurationException {
        Config config = createServerConfig(props);
        return createServer(config);
    }

    /**
     * Configure and create the server.
     */
    public static Server createServer(Config config) throws ConfigurationException {
        // The configuration for the JAX-RS REST service
        ResourceConfig resourceConfig = new ResourceConfig();

        configureApplication(resourceConfig, config);

        MetadataObserver mdObserver = new MetadataObserver(config);
        ProducerPool producerPool = new ProducerPool(config);
        ConsumerManager consumerManager = new ConsumerManager(config, mdObserver);
        Context ctx = new Context(config, mdObserver, producerPool, consumerManager);
        resourceConfig.register(RootResource.class);
        resourceConfig.register(new BrokersResource(ctx));
        resourceConfig.register(new TopicsResource(ctx));
        resourceConfig.register(PartitionsResource.class);
        resourceConfig.register(new ConsumersResource(ctx));

        // Configure the servlet container
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);
        Server server = new Server(config.port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");
        server.setHandler(context);
        return server;
    }

    public static Server createServer() throws ConfigurationException {
        return createServer(createServerConfig(null));
    }


    public static Config createServerConfig(Properties props) throws ConfigurationException {
        if (props == null)
            props = new Properties();
        return new Config(props);
    }

    /**
     * Register standard components for a Kafka REST server application for the configuration, which can be either
     * an ResourceConfig for a server or a ClientConfig for a Jersey-based REST client.
     */
    public static void configureApplication(Configurable<?> config, Config restConfig) {
        config.register(JacksonMessageBodyProvider.class);
        config.register(JsonParseExceptionMapper.class);

        config.register(ValidationFeature.class);
        config.register(ConstraintViolationExceptionMapper.class);
        config.register(new WebApplicationExceptionMapper(restConfig));
        config.register(new GenericExceptionMapper(restConfig));

        config.property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);
    }
}
