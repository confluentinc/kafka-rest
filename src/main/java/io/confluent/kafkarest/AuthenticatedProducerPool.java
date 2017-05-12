/*
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

import static io.confluent.kafkarest.ProducerBuilder.buildAvroConfig;
import static io.confluent.kafkarest.ProducerBuilder.buildAvroProducer;
import static io.confluent.kafkarest.ProducerBuilder.buildBinaryProducer;
import static io.confluent.kafkarest.ProducerBuilder.buildJsonProducer;
import static io.confluent.kafkarest.ProducerBuilder.buildStandardConfig;

import io.confluent.kafkarest.entities.EmbeddedFormat;
import org.apache.kafka.common.config.SaslConfigs;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.core.SecurityContext;

/**
 * Shared pool of authenticated Kafka producers used to send messages.
 */
public class AuthenticatedProducerPool {

  private Map<JaasModule, RestProducer> authenticatedProducers = new HashMap<>();
  private final KafkaRestConfig appConfig;
  private final String bootstrapBrokers;
  private final Properties producerConfigOverrides;

  public AuthenticatedProducerPool(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    this.appConfig = appConfig;
    this.bootstrapBrokers = bootstrapBrokers;
    this.producerConfigOverrides = producerConfigOverrides;
  }

  public RestProducer get(EmbeddedFormat recordFormat, JaasModule jaasModule) {
    if (!authenticatedProducers.containsKey(jaasModule)) {
      RestProducer restProducer = buildAuthenticatedProducer(recordFormat, jaasModule);
      authenticatedProducers.put(jaasModule, restProducer);
      return restProducer;
    }
    return authenticatedProducers.get(jaasModule);
  }

  /**
   * Build an authenticated producer with the given JAAS module
   *
   * @param recordFormat
   *     format
   * @param jaasModule
   *     JAAS Module
   * @return An authenticated producer
   */
  private RestProducer buildAuthenticatedProducer(EmbeddedFormat recordFormat,
                                                  JaasModule jaasModule) {
    RestProducer restProducer;
    switch (recordFormat) {
      case JSON:
        Map<String, Object> jsonProps =
            buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
        jsonProps.put(SaslConfigs.SASL_JAAS_CONFIG, jaasModule.toString());
        restProducer = buildJsonProducer(jsonProps);
        break;
      case AVRO:
        Map<String, Object> avroProps =
            buildAvroConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
        avroProps.put(SaslConfigs.SASL_JAAS_CONFIG, jaasModule.toString());
        restProducer = buildAvroProducer(avroProps);
        break;
      case BINARY:
        Map<String, Object> binaryProps =
            buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
        binaryProps.put(SaslConfigs.SASL_JAAS_CONFIG, jaasModule.toString());
        restProducer = buildBinaryProducer(binaryProps);
        break;
      default:
        throw new RuntimeException("Format " + recordFormat + " is unsupported");
    }
    return restProducer;
  }

  JaasModule getJaasModule(SecurityContext securityContext) {
    Principal userPrincipal = securityContext.getUserPrincipal();
    if (userPrincipal != null) {
      String authenticationMappingClassName = appConfig.getString("authentication.mapping");
      if (authenticationMappingClassName != null && !authenticationMappingClassName.isEmpty()) {
        try {
          AuthenticationMapping authenticationMapping =
              (AuthenticationMapping) Class.forName(authenticationMappingClassName).newInstance();
          return authenticationMapping.getJaasModule(userPrincipal.getName());
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
          throw new RuntimeException("Unable to instance the authentication mapping class "
                                         + authenticationMappingClassName, e);
        }
      }
    }
    return null;
  }
}
