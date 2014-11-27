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
package io.confluent.kafkarest.junit;

import io.confluent.kafkarest.KafkaRestServer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;

import javax.ws.rs.core.Application;
import java.util.List;
import java.util.Vector;

public abstract class EmbeddedServerTestHarness {
    private List<Object> resources = new Vector<>();
    private List<Class<?>> resourceClasses = new Vector<>();

    private JerseyTest test;

    @Before
    public void setUp() throws Exception {
        getJerseyTest().setUp();
    }

    @After
    public void tearDown() throws Exception {
        test.tearDown();
    }

    protected void addResource(Object resource) {
        resources.add(resource);
    }

    protected void addResource(Class<?> resource) {
        resourceClasses.add(resource);
    }

    protected JerseyTest getJerseyTest() {
        // This is instantiated on demand since we need subclasses to register the resources they need passed along,
        // but JerseyTest calls configure() from its constructor.
        if (test == null) {
            test = new JerseyTest() {
                @Override
                protected Application configure() {
                    ResourceConfig config = new ResourceConfig();
                    KafkaRestServer.configureApplication(config, null);
                    for (Object resource : resources)
                        config.register(resource);
                    for (Class<?> resource : resourceClasses)
                        config.register(resource);
                    return config;
                }
                @Override
                protected void configureClient(ClientConfig config) {
                    KafkaRestServer.configureApplication(config, null);
                }
            };
        }
        return test;
    }
}
