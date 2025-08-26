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

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.jakarta.rs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jakarta.rs.base.JsonParseExceptionMapper;
import io.confluent.kafkarest.backends.BackendsModule;
import io.confluent.kafkarest.config.ConfigModule;
import io.confluent.kafkarest.controllers.ControllersModule;
import io.confluent.kafkarest.exceptions.ExceptionsModule;
import io.confluent.kafkarest.exceptions.KafkaRestExceptionMapper;
import io.confluent.kafkarest.extension.EnumConverterProvider;
import io.confluent.kafkarest.extension.InstantConverterProvider;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.ratelimit.RateLimitFeature;
import io.confluent.kafkarest.requestlog.CustomLog;
import io.confluent.kafkarest.requestlog.CustomLogRequestAttributes;
import io.confluent.kafkarest.requestlog.GlobalDosFilterListener;
import io.confluent.kafkarest.requestlog.PerConnectionDosFilterListener;
import io.confluent.kafkarest.resources.ResourcesFeature;
import io.confluent.kafkarest.response.JsonStreamMessageBodyReader;
import io.confluent.kafkarest.response.ResponseModule;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.WebApplicationExceptionMapper;
import jakarta.ws.rs.core.Configurable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.util.StringUtil;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for configuring and running an embedded Kafka server. */
public class KafkaRestApplication extends Application<KafkaRestConfig> {

  private static final Logger log = LoggerFactory.getLogger(KafkaRestApplication.class);

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
    this(config, path, null);
  }

  public KafkaRestApplication(KafkaRestConfig config, String path, String listenerName) {
    this(config, path, listenerName, null /* requestLogWriter */, null /* requestLogFormat */);
  }

  /* This public-constructor exists to facilitate testing with a custom requestLogWriter, and
   * requestLogFormat in an integration test in a different package.
   */
  public KafkaRestApplication(
      KafkaRestConfig config,
      String path,
      String listenerName,
      RequestLog.Writer requestLogWriter,
      String requestLogFormat) {
    super(
        config,
        path,
        listenerName,
        createRequestLog(config, requestLogWriter, requestLogFormat, listenerName));

    restResourceExtensions =
        config.getConfiguredInstances(
            KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG, RestResourceExtension.class);
    config.setMetrics(metrics);

    // Set up listeners for dos-filters, needed for custom-logging for when dos-filter rate-limits.
    this.addNonGlobalDosfilterListener(new PerConnectionDosFilterListener());
    this.addGlobalDosfilterListener(new GlobalDosFilterListener());

    // rest.resource.extension.class is required for Enterprise deployments
    securityResourceExtensionWarning(config);
  }

  private static RequestLog createRequestLog(
      KafkaRestConfig config,
      RequestLog.Writer requestLogWriter,
      String requestLogFormat,
      String listenerName) {
    if (config.getBoolean(KafkaRestConfig.USE_CUSTOM_REQUEST_LOGGING_CONFIG)) {
      log.info("For rest-app with listener {}, configuring custom request logging", listenerName);
      if (requestLogWriter == null) {
        Slf4jRequestLogWriter logWriter = new Slf4jRequestLogWriter();
        logWriter.setLoggerName(config.getString(RestConfig.REQUEST_LOGGER_NAME_CONFIG));
        requestLogWriter = logWriter;
      }

      if (requestLogFormat == null) {
        requestLogFormat = CustomRequestLog.EXTENDED_NCSA_FORMAT + " %{ms}T";
      }

      return new CustomLog(
          requestLogWriter,
          requestLogFormat,
          new String[] {
            CustomLogRequestAttributes.REST_ERROR_CODE,
            CustomLogRequestAttributes.REST_PRODUCE_RECORD_ERROR_CODE_COUNTS
          });
    }
    // Return null, as Application's ctor would set-up a default request-logger.
    return null;
  }

  @Override
  public void configurePreResourceHandling(ServletContextHandler context) {}

  @Override
  public void configurePostResourceHandling(ServletContextHandler context) {}

  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    if (StringUtil.isBlank(appConfig.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG))) {
      throw new RuntimeException(
          KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG + " needs to be configured");
    }

    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    config.register(new JsonStreamMessageBodyReader(getJsonMapper(), appConfig));
    config.register(new BackendsModule());
    config.register(new ConfigModule(appConfig));
    config.register(new ControllersModule());
    config.register(new ExceptionsModule());
    config.register(RateLimitFeature.class);
    config.register(new ResourcesFeature(appConfig));
    config.register(new ResponseModule());

    config.register(ResourceAccesslistFeature.class);

    config.register(EnumConverterProvider.class);
    config.register(InstantConverterProvider.class);

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.register(config, appConfig);
    }
  }

  @Override
  public ObjectMapper getJsonMapper() {
    ObjectMapper mapper =
        super.getJsonMapper()
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .setTimeZone(TimeZone.getTimeZone("UTC"));

    // We could use PRODUCE_REQUEST_SIZE_LIMIT_MAX_BYTES_CONFIG to set the max read length.
    // However, the integration test produceBinaryWithLargerSizeMessage shows that a 20M
    // message needs this value to be set to 23999999 to allow the message to be read.
    // Rather than trying to work out how many extra bytes to add to account for headers etc,
    // we'll let the existing code in JsonStreamMessageBodyReader.readFrom() deal with message
    // length checks, and just use maxint for the limit imposed by jackson.databind from v2.15
    // onwards.
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build());

    return mapper;
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
  }

  private void securityResourceExtensionWarning(KafkaRestConfig config) {
    List<String> extensions = config.getList(KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG);
    for (String extension : extensions) {
      if (!StringUtil.isBlank(extension)
          && extension
              .toLowerCase()
              .contains("io.confluent.kafkarest.security.kafkarestsecurityresourceextension")) {
        return;
      }
    }
    log.warn(
        "REST security extension is not configured. "
            + "If an Enterprise license is expected to be configured, "
            + "please install and activate the security plugins component "
            + "following instructions on this website: "
            + "https://docs.confluent.io/platform/current/confluent-security-plugins/"
            + "kafka-rest.html#kafka-rest-security-plugins-install. "
            + "Confluent does not offer Enterprise support for any self-managed "
            + "(Confluent Platform) components without a valid Enterprise license. "
            + "Please ignore this warning if not using an Enterprise edition of this software.");
  }
}
