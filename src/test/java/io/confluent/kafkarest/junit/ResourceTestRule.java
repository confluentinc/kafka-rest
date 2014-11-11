package io.confluent.kafkarest.junit;

// Adapted from ResourceTestRule in DropWizard.

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.validation.ValidationFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Application;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A JUnit {@link TestRule} for testing Jersey resources.
 */
public class ResourceTestRule implements TestRule {
    public static class Builder {

        private final Set<Object> singletons = new HashSet<Object>();
        private final Set<Class<?>> providers = new HashSet<Class<?>>();
        private final Map<String, Object> properties = new HashMap<String, Object>();
        private ObjectMapper mapper = new ObjectMapper();
        private Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

        public Builder setMapper(ObjectMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public Builder setValidator(Validator validator) {
            this.validator = validator;
            return this;
        }

        public Builder addResource(Object resource) {
            singletons.add(resource);
            return this;
        }

        public Builder addProvider(Class<?> klass) {
            providers.add(klass);
            return this;
        }

        public Builder addProvider(Object provider) {
            singletons.add(provider);
            return this;
        }

        public Builder addProperty(String property, Object value) {
            properties.put(property, value);
            return this;
        }

        public ResourceTestRule build() {
            return new ResourceTestRule(singletons, providers, properties, mapper, validator);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final Set<Object> singletons;
    private final Set<Class<?>> providers;
    private final Map<String, Object> properties;
    private final ObjectMapper mapper;
    private final Validator validator;

    private JerseyTest test;

    private ResourceTestRule(Set<Object> singletons,
                             Set<Class<?>> providers,
                             Map<String, Object> properties,
                             ObjectMapper mapper,
                             Validator validator) {
        this.singletons = singletons;
        this.providers = providers;
        this.properties = properties;
        this.mapper = mapper;
        this.validator = validator;
    }

    public Validator getValidator() {
        return validator;
    }

    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    public Client client() {
        return test.client();
    }

    public JerseyTest getJerseyTest() {
        return test;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    test = new JerseyTest() {
                        @Override
                        protected Application configure() {
                            ResourceConfig config = new ResourceConfig();
                            for (Class<?> provider : providers) {
                                config.register(provider);
                            }
                            for (Map.Entry<String, Object> property : properties.entrySet()) {
                                config.property(property.getKey(), property.getValue());
                            }
                            config.register(JacksonJaxbJsonProvider.class)
                                    .register(JacksonFeature.class)
                                    .register(ValidationFeature.class)
                                    .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);
                            for (Object singleton : singletons)
                                config.register(singleton);
                            return config;
                        }
                    };
                    test.setUp();
                    base.evaluate();
                } finally {
                    if (test != null) {
                        test.tearDown();
                    }
                }
            }
        };
    }
}
