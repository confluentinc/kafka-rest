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

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.converters.ProtobufConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<EmbeddedFormat, RestProducer> producers =
      new HashMap<EmbeddedFormat, RestProducer>();

  public ProducerPool(KafkaRestConfig appConfig) {
    this(appConfig, null);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      Properties producerConfigOverrides
  ) {
    this(appConfig, RestConfigUtils.bootstrapBrokers(appConfig), producerConfigOverrides);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {

    Map<String, Object> binaryProps =
        buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.BINARY, buildBinaryProducer(binaryProps));

    Map<String, Object> jsonProps =
        buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.JSON, buildJsonProducer(jsonProps));

    Map<String, Object> avroProps =
        buildSchemaConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.AVRO, buildAvroProducer(avroProps));

    Map<String, Object> jsonSchemaProps =
        buildSchemaConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.JSONSCHEMA, buildJsonSchemaProducer(jsonSchemaProps));

    Map<String, Object> protobufProps =
        buildSchemaConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.PROTOBUF, buildProtobufProducer(protobufProps));
  }

  private Map<String, Object> buildStandardConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    Properties producerProps = (Properties) appConfig.getProducerProperties();
    return buildConfig(props, producerProps, producerConfigOverrides);
  }

  private NoSchemaRestProducer<byte[], byte[]> buildBinaryProducer(
      Map<String, Object>
          binaryProps
  ) {
    return buildNoSchemaProducer(binaryProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private NoSchemaRestProducer<Object, Object> buildJsonProducer(Map<String, Object> jsonProps) {
    return buildNoSchemaProducer(jsonProps, new KafkaJsonSerializer(), new KafkaJsonSerializer());
  }

  private <K, V> NoSchemaRestProducer<K, V> buildNoSchemaProducer(
      Map<String, Object> props,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer
  ) {
    keySerializer.configure(props, true);
    valueSerializer.configure(props, false);
    KafkaProducer<K, V> producer =
        new KafkaProducer<K, V>(props, keySerializer, valueSerializer);
    return new NoSchemaRestProducer<K, V>(producer);
  }

  private Map<String, Object> buildSchemaConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> schemaDefaults = new HashMap<String, Object>();
    schemaDefaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    schemaDefaults.put(
        "schema.registry.url",
        appConfig.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)
    );

    Properties producerProps = (Properties) appConfig.getProducerProperties();
    return buildConfig(schemaDefaults, producerProps, producerConfigOverrides);
  }

  private SchemaRestProducer buildAvroProducer(Map<String, Object> props) {
    final KafkaAvroSerializer keySerializer = new KafkaAvroSerializer();
    keySerializer.configure(props, true);
    final KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer();
    valueSerializer.configure(props, false);
    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, keySerializer, valueSerializer);
    return new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new AvroSchemaProvider(), new AvroConverter());
  }

  private SchemaRestProducer buildJsonSchemaProducer(Map<String, Object> props) {
    final KafkaJsonSchemaSerializer keySerializer = new KafkaJsonSchemaSerializer();
    keySerializer.configure(props, true);
    final KafkaJsonSchemaSerializer valueSerializer = new KafkaJsonSchemaSerializer();
    valueSerializer.configure(props, false);
    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, keySerializer, valueSerializer);
    return new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new JsonSchemaProvider(), new JsonSchemaConverter());
  }

  private SchemaRestProducer buildProtobufProducer(Map<String, Object> props) {
    final KafkaProtobufSerializer keySerializer = new KafkaProtobufSerializer();
    keySerializer.configure(props, true);
    final KafkaProtobufSerializer valueSerializer = new KafkaProtobufSerializer();
    valueSerializer.configure(props, false);
    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, keySerializer, valueSerializer);
    return new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new ProtobufSchemaProvider(), new ProtobufConverter());
  }

  private Map<String, Object> buildConfig(
      Map<String, Object> defaults,
      Properties userProps,
      Properties overrides
  ) {
    // Note careful ordering: built-in values we look up automatically first, then configs
    // specified by user with initial KafkaRestConfig, and finally explicit overrides passed to
    // this method (only used for tests)
    Map<String, Object> config = new HashMap<String, Object>(defaults);
    for (String propName : userProps.stringPropertyNames()) {
      config.put(propName, userProps.getProperty(propName));
    }
    if (overrides != null) {
      for (String propName : overrides.stringPropertyNames()) {
        config.put(propName, overrides.getProperty(propName));
      }
    }
    return config;
  }

  public <K, V> void produce(
      String topic,
      Integer partition,
      EmbeddedFormat recordFormat,
      ProduceRequest<K, V> produceRequest,
      ProduceRequestCallback callback
  ) {
    ProduceTask task =
        new ProduceTask(
            produceRequest,
            produceRequest.getRecords().size(),
            callback);
    log.trace("Starting produce task " + task.toString());
    @SuppressWarnings("unchecked")
    RestProducer<K, V> restProducer = (RestProducer<K, V>) producers.get(recordFormat);
    restProducer.produce(
        task,
        topic,
        partition,
        produceRequest.getRecords());
  }

  public void shutdown() {
    for (RestProducer restProducer : producers.values()) {
      restProducer.close();
    }
  }

  public interface ProduceRequestCallback {

    /**
     * Invoked when all messages have either been recorded or received an error
     *
     * @param results list of responses, in the same order as the request. Each entry can be either
     *                a RecordAndMetadata for successful responses or an exception
     */
    public void onCompletion(
        Integer keySchemaId,
        Integer valueSchemaId,
        List<RecordMetadataOrException> results
    );
  }
}
