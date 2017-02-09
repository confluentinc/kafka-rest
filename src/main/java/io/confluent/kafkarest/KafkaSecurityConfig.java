/**
 * Copyright 2017 Confluent Inc.
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
import java.util.List;
import java.util.Arrays;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.clients.CommonClientConfigs;

/**
 * Helper class to extract Kafka security settings 
 */
public class KafkaSecurityConfig  {

  public static void addSecurityConfigsToClientProperties(KafkaRestConfig config, Properties props) {
      //  first clear the ssl and sasl properties from props, which are copied from KafkaRestConfig
      //  and used for REST Proxy SSL set ups.
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                config.getString(KafkaRestConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG));
      clearSslConfigs(props);
      clearSaslConfigs(props);
      addSslConfigsToClientProperties(config, props);
      addSaslConfigsToClientProperties(config, props);
      clearKafkaStoreSslConfigs(props);
      clearKafkaStoreSaslConfigs(props);
  }

  public static void removeKeys(Properties props, List<String>keys) {
      for (String key: keys) {
	  props.remove(key);
      }
  }
  public static void clearSslConfigs(Properties props) {
      List<String> keys =
	  Arrays.asList(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
			SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
			SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
			SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
			SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
			SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
			SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
			SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
			SslConfigs.SSL_KEY_PASSWORD_CONFIG,
			SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
			SslConfigs.SSL_PROTOCOL_CONFIG,
			SslConfigs.SSL_PROVIDER_CONFIG,
			SslConfigs.SSL_CIPHER_SUITES_CONFIG,
			SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);

      removeKeys(props, keys);
  }
    
  public static void clearSaslConfigs(Properties props) {
      List<String> keys =
	  Arrays.asList(SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
			SaslConfigs.SASL_MECHANISM,
			SaslConfigs.SASL_KERBEROS_KINIT_CMD,
			SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
			SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,
			SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
      
      removeKeys(props, keys);
  }

  public static void addSslConfigsToClientProperties(KafkaRestConfig config, Properties props) {
    if (config.getString(KafkaRestConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
            SecurityProtocol.SSL.toString()) ||
            config.getString(KafkaRestConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
                    SecurityProtocol.SASL_SSL.toString())) {
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG));
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG));
      props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG));
      props.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
      putIfNotEmptyString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG), props);
      putIfNotEmptyString(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG), props);
      props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG));
      props.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG));
      putIfNotEmptyString(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_KEY_PASSWORD_CONFIG), props);
      putIfNotEmptyString(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG), props);
      props.put(SslConfigs.SSL_PROTOCOL_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_PROTOCOL_CONFIG));
      putIfNotEmptyString(SslConfigs.SSL_PROVIDER_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_PROVIDER_CONFIG), props);
      putIfNotEmptyString(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_CIPHER_SUITES_CONFIG), props);
      putIfNotEmptyString(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
              config.getString(KafkaRestConfig.KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), props);
    }
  }

  public static void addSaslConfigsToClientProperties(KafkaRestConfig config, Properties props) {
    if (config.getString(KafkaRestConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
            SecurityProtocol.SASL_PLAINTEXT.toString()) ||
            config.getString(KafkaRestConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
                    SecurityProtocol.SASL_SSL.toString())) {
      putIfNotEmptyString(SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
              config.getString(KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG), props);
      props.put(SaslConfigs.SASL_MECHANISM,
              config.getString(KafkaRestConfig.KAFKASTORE_SASL_MECHANISM_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_KINIT_CMD,
              config.getString(KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
              config.getLong(KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,
              config.getDouble(KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
              config.getDouble(KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG));
    }
  }

  public static void clearKafkaStoreSslConfigs(Properties props) {
      List<String> keys =
	  Arrays.asList(KafkaRestConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_KEY_PASSWORD_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_PROTOCOL_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_PROVIDER_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_CIPHER_SUITES_CONFIG,
			KafkaRestConfig.KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
      
      removeKeys(props, keys);
  }

  public static void clearKafkaStoreSaslConfigs(Properties props) {
      List<String> keys =
	  Arrays.asList(KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG,
			KafkaRestConfig.KAFKASTORE_SASL_MECHANISM_CONFIG,
			KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG,
			KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG,
			KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG,
			KafkaRestConfig.KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG);
      removeKeys(props, keys);
  }

  // helper method to only add a property if its not the empty string. This is required
  // because some Kafka client configs expect a null default value, yet ConfigDef doesn't
  // support null default values.
  private static void putIfNotEmptyString(String parameter, String value, Properties props) {
    if (!value.trim().isEmpty()) {
      props.put(parameter, value);
    }
  }

}
