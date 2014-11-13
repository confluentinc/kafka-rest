package io.confluent.kafkarest;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import io.confluent.kafkarest.resources.BrokersResource;
import io.confluent.kafkarest.resources.TopicsResource;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.validation.ValidationFeature;
import org.glassfish.jersey.servlet.ServletContainer;

import java.io.IOException;
import java.util.Properties;

public class Main {
    /**
     * Configure and create the server.
     */
    public static Server createServer(Properties props) {
        // The configuration for the JAX-RS REST service
        ResourceConfig resourceConfig = new ResourceConfig();

        // Setup Jackson-based JSON support
        resourceConfig.register(JacksonJaxbJsonProvider.class)
                .register(JacksonFeature.class)
                .register(ValidationFeature.class)
                .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        // Register all REST resources
        if (props == null)
            props = new Properties();
        Config config = new Config(props);
        MetadataObserver mdObserver = new MetadataObserver(config);
        ProducerPool producerPool = new ProducerPool(config);
        Context ctx = new Context(config, mdObserver, producerPool);
        resourceConfig.register(new BrokersResource(ctx));
        resourceConfig.register(new TopicsResource(ctx));

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

    public static Server createServer() {
        return createServer(null);
    }

    /**
     * Starts an embedded Jetty server running the REST server.
     */
    public static void main(String[] args) throws IOException {
        Server server = createServer();

        // Finally, run the server
        try {
            server.start();
            System.out.println("Server started, listening for requests...");
            server.join();
        } catch (Exception e) {
            System.err.println("Server died unexpectedly: " + e.toString());
        }
    }
}

