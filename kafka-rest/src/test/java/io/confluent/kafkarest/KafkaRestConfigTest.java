/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.kafkarest.KafkaRestConfig.METRICS_REPORTER_CLASSES_CONFIG;
import static io.confluent.kafkarest.KafkaRestMetricsContext.KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT;
import static io.confluent.kafkarest.KafkaRestMetricsContext.RESOURCE_LABEL_CLUSTER_ID;
import static io.confluent.kafkarest.KafkaRestMetricsContext.RESOURCE_LABEL_COMMIT_ID;
import static io.confluent.kafkarest.KafkaRestMetricsContext.RESOURCE_LABEL_VERSION;
import static java.util.Collections.singletonMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.rest.metrics.RestMetricsContext;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KafkaRestConfigTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getMetricsContext_propagateMetricsConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "context_cluster_id");

    RestMetricsContext config = new KafkaRestConfig(properties, doLog).getMetricsContext();

    assertEquals(AppInfoParser.getCommitId(), config.getLabel(RESOURCE_LABEL_COMMIT_ID));
    assertEquals(AppInfoParser.getVersion(), config.getLabel(RESOURCE_LABEL_VERSION));
    assertEquals("context_cluster_id", config.getLabel(RESOURCE_LABEL_CLUSTER_ID));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getProducerProperties_propagateMetrics_defaultClusterId(boolean doLog) {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getProducerProperties();

    assertEquals(
        KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
        producerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getProducerProperties_propagateMetricsProperties(boolean doLog) {
    Properties properties = new Properties();
    properties.put(
        METRICS_REPORTER_CLASSES_CONFIG,
        "metrics.reporter.1, metrics.reporter.2, metrics.reporter.3");
    properties.put(reporter_config("api.key"), "my_api_key");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "producer_cluster_id");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getProducerProperties();
    assertEquals(
        Arrays.asList("metrics.reporter.1", "metrics.reporter.2", "metrics.reporter.3"),
        producerProperties.get(METRICS_REPORTER_CLASSES_CONFIG));
    assertEquals("my_api_key", producerProperties.get(reporter_config("api.key")));
    assertEquals(
        AppInfoParser.getCommitId(),
        producerProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(
        AppInfoParser.getVersion(), producerProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals(
        "producer_cluster_id", producerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getConsumerProperties_propagatesSchemaRegistryProperties(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "https://schemaregistry:8085");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals("https://schemaregistry:8085", consumerProperties.get("schema.registry.url"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getConsumerProperties_propagateMetrics_defaultClusterId(boolean doLog) {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties consumerProperties = config.getConsumerProperties();

    assertEquals(
        KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
        consumerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getConsumerProperties_propagateMetricsProperties(boolean doLog) {
    Properties properties = new Properties();
    properties.put(
        METRICS_REPORTER_CLASSES_CONFIG,
        "metrics.reporter.1, metrics.reporter.2, metrics.reporter.3");
    properties.put(reporter_config("api.key"), "my_api_key");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "consumer_cluster_id");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties consumerProperties = config.getConsumerProperties();
    assertEquals(
        Arrays.asList("metrics.reporter.1", "metrics.reporter.2", "metrics.reporter.3"),
        consumerProperties.get(METRICS_REPORTER_CLASSES_CONFIG));
    assertEquals("my_api_key", consumerProperties.get("confluent.telemetry.api.key"));
    assertEquals(
        AppInfoParser.getCommitId(),
        consumerProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(
        AppInfoParser.getVersion(), consumerProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals(
        "consumer_cluster_id", consumerProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getAdminProperties_propagateMetrics_defaultClusterId(boolean doLog) {
    Properties properties = new Properties();

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties adminProperties = config.getAdminProperties();

    assertEquals(
        KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT,
        adminProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getAdminProperties_propagateMetricsProperties(boolean doLog) {
    Properties properties = new Properties();
    properties.put(
        METRICS_REPORTER_CLASSES_CONFIG,
        "metrics.reporter.1, metrics.reporter.2, metrics.reporter.3");
    properties.put(reporter_config("api.key"), "my_api_key");
    properties.put(context_config(RESOURCE_LABEL_CLUSTER_ID), "admin_cluster_id");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties adminProperties = config.getAdminProperties();
    assertEquals(
        Arrays.asList("metrics.reporter.1", "metrics.reporter.2", "metrics.reporter.3"),
        adminProperties.get(METRICS_REPORTER_CLASSES_CONFIG));
    assertEquals("my_api_key", adminProperties.get("confluent.telemetry.api.key"));
    assertEquals(
        AppInfoParser.getCommitId(), adminProperties.get(context_config(RESOURCE_LABEL_COMMIT_ID)));
    assertEquals(
        AppInfoParser.getVersion(), adminProperties.get(context_config(RESOURCE_LABEL_VERSION)));
    assertEquals(
        "admin_cluster_id", adminProperties.get(context_config(RESOURCE_LABEL_CLUSTER_ID)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getConsumerProperties_propagatesSchemaRegistryConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getConsumerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("basic.auth.user.info"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getConsumerProperties_propagatesClientSchemaRegistryConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("client.schema.registry.url", "foobar");
    properties.put("client.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getConsumerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getConsumerProperties_propagatesConsumerSchemaRegistryConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("consumer.schema.registry.url", "foobar");
    properties.put("consumer.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getConsumerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getProducerProperties_propagatesSchemaRegistryConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getProducerProperties_propagatesClientSchemaRegistryConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("client.schema.registry.url", "foobar");
    properties.put("client.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getProducerProperties_propagatesProducerSchemaRegistryConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("producer.schema.registry.url", "foobar");
    properties.put("producer.schema.registry.basic.auth.user.info", "fozbaz");
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    Properties producerProperties = config.getProducerProperties();
    assertEquals("foobar", producerProperties.get("schema.registry.url"));
    assertEquals("fozbaz", producerProperties.get("schema.registry.basic.auth.user.info"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getJsonSerializerConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("producer.acks", 1);
    properties.put("producer.json.indent.output", true);
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    assertEquals(singletonMap("json.indent.output", true), config.getJsonSerializerConfigs());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getAvroSerializerConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("producer.acks", 1);
    // this is one of specific avro configs, added by stripping "producer." prefix
    properties.put("producer.avro.use.logical.type.converters", true);
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    assertEquals(
        ImmutableMap.of(
            "schema.registry.url", "foobar",
            "auto.register.schemas", false,
            "use.latest.version", false,
            "avro.use.logical.type.converters", true),
        config.getAvroSerializerConfigs());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getJsonschemaSerializerConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("client.json.fail.invalid.schema", true);
    properties.put("producer.acks", 1);
    properties.put("producer.json.fail.invalid.schema", true);
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    assertEquals(
        ImmutableMap.of(
            "schema.registry.url", "foobar",
            "auto.register.schemas", false,
            "use.latest.version", false,
            "json.fail.invalid.schema", true),
        config.getJsonschemaSerializerConfigs());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void getProtobufSerializerConfigs(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "foobar");
    properties.put("producer.acks", 1);
    properties.put(
        "producer.reference.subject.name.strategy", DefaultReferenceSubjectNameStrategy.class);
    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);

    assertEquals(
        ImmutableMap.of(
            "schema.registry.url",
            "foobar",
            "auto.register.schemas",
            false,
            "use.latest.version",
            false,
            "reference.subject.name.strategy",
            DefaultReferenceSubjectNameStrategy.class),
        config.getProtobufSerializerConfigs());
  }

  @Test
  public void testMapToStringHideSensitiveConfigs() {
    AbstractConfig mockAbstractConfig = mock(AbstractConfig.class);
    Map<String, Object> configs =
        ImmutableMap.of(
            // this config belongs to passwordTypeConfigs field
            "sasl.jaas.config", "secret config",
            // this config is defined as "PASSWORD" type
            "ssl.truststore.password", "secret password",
            // this config is defined as "LONG" type
            "max.request.size", "100");

    expect(mockAbstractConfig.typeOf("ssl.truststore.password")).andReturn(Type.PASSWORD);
    expect(mockAbstractConfig.getPassword("ssl.truststore.password"))
        .andReturn(new Password("secret password"));
    expect(mockAbstractConfig.typeOf("max.request.size")).andReturn(Type.LONG);
    replay(mockAbstractConfig);

    String ret = KafkaRestConfig.mapToStringHideSensitiveConfigs(configs, mockAbstractConfig);

    verify(mockAbstractConfig);
    assertThat(ret, containsString("sasl.jaas.config=" + Password.HIDDEN));
    assertThat(ret, containsString("ssl.truststore.password=" + Password.HIDDEN));
    assertThat(ret, containsString("max.request.size=100"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetAdminProperties_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    // below configs will be stripped down prefix "client" or "admin"
    properties.put("admin.sasl.jaas.config", "super secret sasl.jaas.config");
    properties.put("client.ssl.truststore.password", "super secret");
    properties.put("client.sasl.kerberos.kinit.cmd", "/usr/bin/kinit");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Properties adminProperties = config.getAdminProperties();

    // check that the config values can be retrieved as normal
    assertEquals("super secret sasl.jaas.config", adminProperties.get("sasl.jaas.config"));
    assertEquals("super secret", adminProperties.get("ssl.truststore.password"));
    assertEquals("/usr/bin/kinit", adminProperties.get("sasl.kerberos.kinit.cmd"));

    // check that toString function can hide sensitive configs
    String loggedToString = adminProperties.toString();
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("ssl.truststore.password=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("sasl.kerberos.kinit.cmd=/usr/bin/kinit"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetProducerProperties_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    // below configs will be stripped down prefix "client" or "producer"
    properties.put("producer.sasl.jaas.config", "super secret sasl.jaas.config");
    properties.put("producer.max.request.size", "1024");
    properties.put("client.ssl.truststore.password", "super secret");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Properties producerProperties = config.getProducerProperties();

    // check that the config values can be retrieved as normal
    assertEquals("super secret sasl.jaas.config", producerProperties.get("sasl.jaas.config"));
    assertEquals("super secret", producerProperties.get("ssl.truststore.password"));
    assertEquals("1024", producerProperties.get("max.request.size"));

    // check that toString function can hide sensitive configs
    String loggedToString = producerProperties.toString();
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("max.request.size=1024"));
    assertThat(loggedToString, containsString("ssl.truststore.password=" + Password.HIDDEN));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetProducerConfigs_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    // below configs will be stripped down prefix "client" or "producer"
    properties.put("producer.sasl.jaas.config", "super secret sasl.jaas.config");
    properties.put("producer.max.request.size", "1024");
    properties.put("client.ssl.truststore.password", "super secret");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Map<String, Object> producerConfigs = config.getProducerConfigs();

    // check that the config values can be retrieved as normal
    assertEquals("super secret sasl.jaas.config", producerConfigs.get("sasl.jaas.config"));
    assertEquals("super secret", producerConfigs.get("ssl.truststore.password"));
    assertEquals("1024", producerConfigs.get("max.request.size"));

    // check that toString function can hide sensitive configs
    String loggedToString = producerConfigs.toString();
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("max.request.size=1024"));
    assertThat(loggedToString, containsString("ssl.truststore.password=" + Password.HIDDEN));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetConsumerProperties_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    // below configs will be stripped down prefix "client" or "consumer"
    properties.put("consumer.sasl.jaas.config", "super secret sasl.jaas.config");
    properties.put("consumer.max.poll.records", "500");
    properties.put("client.ssl.truststore.password", "super secret");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Properties consumerProperties = config.getConsumerProperties();

    // check that the config values can be retrieved as normal
    assertEquals("super secret sasl.jaas.config", consumerProperties.get("sasl.jaas.config"));
    assertEquals("super secret", consumerProperties.get("ssl.truststore.password"));
    assertEquals("500", consumerProperties.get("max.poll.records"));

    // check that toString function can hide sensitive configs
    String loggedToString = consumerProperties.toString();
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("max.poll.records=500"));
    assertThat(loggedToString, containsString("ssl.truststore.password=" + Password.HIDDEN));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetSchemaRegistryConfigs_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "http://localhost:8081");
    properties.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
    properties.put("schema.registry.basic.auth.user.info", "base64 encoded secret");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Map<String, Object> schemaRegistryConfigs = config.getSchemaRegistryConfigs();

    // check that the config values can be retrieved as normal
    assertEquals("http://localhost:8081", schemaRegistryConfigs.get("schema.registry.url"));
    assertEquals("USER_INFO", schemaRegistryConfigs.get("basic.auth.credentials.source"));
    assertEquals("base64 encoded secret", schemaRegistryConfigs.get("basic.auth.user.info"));

    // check that toString function can hide sensitive configs
    String loggedToString = schemaRegistryConfigs.toString();
    assertThat(loggedToString, containsString("schema.registry.url=http://localhost:8081"));
    assertThat(loggedToString, containsString("basic.auth.user.info=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("basic.auth.credentials.source=USER_INFO"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetAvroSerializerConfigs_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "http://localhost:8081");
    properties.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
    properties.put("schema.registry.basic.auth.user.info", "base64 encoded secret");
    properties.put("schema.registry.max.schemas.per.subject", "1000");
    properties.put("producer.sasl.jaas.config", "super secret sasl.jaas.config");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Map<String, Object> serializerConfigs = config.getAvroSerializerConfigs();

    // check that the config values can be retrieved as normal
    assertEquals("http://localhost:8081", serializerConfigs.get("schema.registry.url"));
    assertEquals("USER_INFO", serializerConfigs.get("basic.auth.credentials.source"));
    assertEquals("base64 encoded secret", serializerConfigs.get("basic.auth.user.info"));
    assertEquals("1000", serializerConfigs.get("max.schemas.per.subject"));
    assertEquals("super secret sasl.jaas.config", serializerConfigs.get("sasl.jaas.config"));

    // check that toString function can hide sensitive configs
    String loggedToString = serializerConfigs.toString();
    assertThat(loggedToString, containsString("schema.registry.url=http://localhost:8081"));
    assertThat(loggedToString, containsString("basic.auth.user.info=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("basic.auth.credentials.source=USER_INFO"));
    assertThat(loggedToString, containsString("max.schemas.per.subject=1000"));
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetJsonschemaSerializerConfigs_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "http://localhost:8081");
    properties.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
    properties.put("schema.registry.basic.auth.user.info", "base64 encoded secret");
    properties.put("producer.json.oneof.for.nullables", "true");
    properties.put("producer.sasl.jaas.config", "super secret sasl.jaas.config");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Map<String, Object> serializerConfigs = config.getJsonschemaSerializerConfigs();

    // check that the config values can be retrieved as normal
    assertEquals("http://localhost:8081", serializerConfigs.get("schema.registry.url"));
    assertEquals("USER_INFO", serializerConfigs.get("basic.auth.credentials.source"));
    assertEquals("base64 encoded secret", serializerConfigs.get("basic.auth.user.info"));
    assertEquals("true", serializerConfigs.get("json.oneof.for.nullables"));
    assertEquals("super secret sasl.jaas.config", serializerConfigs.get("sasl.jaas.config"));

    // check that toString function can hide sensitive configs
    String loggedToString = serializerConfigs.toString();
    assertThat(loggedToString, containsString("schema.registry.url=http://localhost:8081"));
    assertThat(loggedToString, containsString("basic.auth.user.info=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("basic.auth.credentials.source=USER_INFO"));
    assertThat(loggedToString, containsString("json.oneof.for.nullables=true"));
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetProtobufSerializerConfigs_HideSensitiveInfo(boolean doLog) {
    Properties properties = new Properties();
    properties.put("schema.registry.url", "http://localhost:8081");
    properties.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
    properties.put("schema.registry.basic.auth.user.info", "base64 encoded secret");
    properties.put(
        "producer.reference.subject.name.strategy",
        "io.confluent.kafka.serializers.subject.QualifiedReferenceSubjectNameStrategy");
    properties.put("producer.sasl.jaas.config", "super secret sasl.jaas.config");

    KafkaRestConfig config = new KafkaRestConfig(properties, doLog);
    Map<String, Object> serializerConfigs = config.getProtobufSerializerConfigs();

    // check that the config values can be retrieved as normal
    assertEquals("http://localhost:8081", serializerConfigs.get("schema.registry.url"));
    assertEquals("USER_INFO", serializerConfigs.get("basic.auth.credentials.source"));
    assertEquals("base64 encoded secret", serializerConfigs.get("basic.auth.user.info"));
    assertEquals(
        "io.confluent.kafka.serializers.subject.QualifiedReferenceSubjectNameStrategy",
        serializerConfigs.get("reference.subject.name.strategy"));
    assertEquals("super secret sasl.jaas.config", serializerConfigs.get("sasl.jaas.config"));

    // check that toString function can hide sensitive configs
    String loggedToString = serializerConfigs.toString();
    assertThat(loggedToString, containsString("schema.registry.url=http://localhost:8081"));
    assertThat(loggedToString, containsString("basic.auth.user.info=" + Password.HIDDEN));
    assertThat(loggedToString, containsString("basic.auth.credentials.source=USER_INFO"));
    assertThat(
        loggedToString,
        containsString(
            "reference.subject.name.strategy="
                + "io.confluent.kafka.serializers.subject.QualifiedReferenceSubjectNameStrategy"));
    assertThat(loggedToString, containsString("sasl.jaas.config=" + Password.HIDDEN));
  }

  private String context_config(String suffix) {
    return CommonClientConfigs.METRICS_CONTEXT_PREFIX + suffix;
  }

  private String reporter_config(String suffix) {
    return KafkaRestConfig.TELEMETRY_PREFIX + suffix;
  }
}
