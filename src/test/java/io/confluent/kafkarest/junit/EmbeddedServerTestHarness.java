package io.confluent.kafkarest.junit;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.validation.ValidationFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
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
                    config.register(JacksonJaxbJsonProvider.class)
                            .register(JacksonFeature.class)
                            .register(ValidationFeature.class)
                            .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);
                    for (Object resource : resources)
                        config.register(resource);
                    for (Class<?> resource : resourceClasses)
                        config.register(resource);
                    return config;
                }
            };
        }
        return test;
    }
}
