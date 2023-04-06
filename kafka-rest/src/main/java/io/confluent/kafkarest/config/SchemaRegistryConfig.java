/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.config;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafkarest.entities.EmbeddedFormat;
import org.apache.kafka.common.config.ConfigDef;

public final class SchemaRegistryConfig extends AbstractKafkaSchemaSerDeConfig {
  public static final List<SchemaProvider> SCHEMA_PROVIDERS =
      Collections.unmodifiableList(Arrays.asList(
        EmbeddedFormat.AVRO.getSchemaProvider(),
        EmbeddedFormat.JSONSCHEMA.getSchemaProvider(),
        EmbeddedFormat.PROTOBUF.getSchemaProvider()));
  private static final ConfigDef CONFIG_DEF = baseConfigDef();

  public SchemaRegistryConfig(Map<String, Object> configs) {
    super(CONFIG_DEF, configs);
  }

  public SubjectNameStrategy getSubjectNameStrategy() {
    return new SubjectNameStrategyImpl(
        (SubjectNameStrategy) keySubjectNameStrategy(),
        (SubjectNameStrategy) valueSubjectNameStrategy());
  }

  private static final class SubjectNameStrategyImpl implements SubjectNameStrategy {
    private final SubjectNameStrategy keySubjectNameStrategy;
    private final SubjectNameStrategy valueSubjectNameStrategy;

    private SubjectNameStrategyImpl(
        SubjectNameStrategy keySubjectNameStrategy, SubjectNameStrategy valueSubjectNameStrategy) {
      this.keySubjectNameStrategy = requireNonNull(keySubjectNameStrategy);
      this.valueSubjectNameStrategy = requireNonNull(valueSubjectNameStrategy);
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
      if (isKey) {
        return keySubjectNameStrategy.subjectName(topic, isKey, schema);
      } else {
        return valueSubjectNameStrategy.subjectName(topic, isKey, schema);
      }
    }

    @Override
    public void configure(Map<String, ?> configs) {
      keySubjectNameStrategy.configure(configs);
      valueSubjectNameStrategy.configure(configs);
    }
  }
}
