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

package io.confluent.kafkarest.controllers;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.config.ConfigModule.AvroSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.JsonschemaSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.ProtobufSerializerConfigs;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/** A module to install the various controllers required by the application. */
public final class ControllersModule extends AbstractBinder {

  protected void configure() {
    bind(AclManagerImpl.class).to(AclManager.class);
    bind(BrokerConfigManagerImpl.class).to(BrokerConfigManager.class);
    bind(BrokerManagerImpl.class).to(BrokerManager.class);
    bind(ClusterConfigManagerImpl.class).to(ClusterConfigManager.class);
    bind(ClusterManagerImpl.class).to(ClusterManager.class);
    bind(ConsumerAssignmentManagerImpl.class).to(ConsumerAssignmentManager.class);
    bind(ConsumerGroupLagSummaryManagerImpl.class).to(ConsumerGroupLagSummaryManager.class);
    bind(ConsumerGroupManagerImpl.class).to(ConsumerGroupManager.class);
    bind(ConsumerLagManagerImpl.class).to(ConsumerLagManager.class);
    bind(ConsumerManagerImpl.class).to(ConsumerManager.class);
    bindAsContract(NoSchemaRecordSerializer.class).in(Singleton.class);
    bind(PartitionManagerImpl.class).to(PartitionManager.class);
    bind(ProduceControllerImpl.class).to(ProduceController.class);
    bind(ReassignmentManagerImpl.class).to(ReassignmentManager.class);
    bind(RecordSerializerFacade.class).to(RecordSerializer.class);
    bind(ReplicaManagerImpl.class).to(ReplicaManager.class);
    bindFactory(SchemaManagerFactory.class).to(SchemaManager.class);
    bind(TopicConfigManagerImpl.class).to(TopicConfigManager.class);
    bind(TopicManagerImpl.class).to(TopicManager.class);
    bindFactory(SchemaRecordSerializerFactory.class).to(SchemaRecordSerializer.class);
  }

  private static final class SchemaRecordSerializerFactory
      implements Factory<SchemaRecordSerializer> {

    private final Optional<SchemaRegistryClient> schemaRegistryClient;
    private final Map<String, Object> avroSerializerConfigs;
    private final Map<String, Object> jsonschemaSerializerConfigs;
    private final Map<String, Object> protobufSerializerConfigs;

    @Inject
    private SchemaRecordSerializerFactory(
        Optional<SchemaRegistryClient> schemaRegistryClient,
        @AvroSerializerConfigs Map<String, Object> avroSerializerConfigs,
        @JsonschemaSerializerConfigs Map<String, Object> jsonschemaSerializerConfigs,
        @ProtobufSerializerConfigs Map<String, Object> protobufSerializerConfigs) {
      this.schemaRegistryClient = requireNonNull(schemaRegistryClient);
      this.avroSerializerConfigs = requireNonNull(avroSerializerConfigs);
      this.jsonschemaSerializerConfigs = requireNonNull(jsonschemaSerializerConfigs);
      this.protobufSerializerConfigs = requireNonNull(protobufSerializerConfigs);
    }

    @Override
    public SchemaRecordSerializer provide() {
      if (schemaRegistryClient.isPresent()) {
        return new SchemaRecordSerializerImpl(
            schemaRegistryClient.get(),
            avroSerializerConfigs,
            jsonschemaSerializerConfigs,
            protobufSerializerConfigs);
      } else {
        return new SchemaRecordSerializerThrowing();
      }
    }

    @Override
    public void dispose(SchemaRecordSerializer schemaRecordSerializer) {}
  }

  private static final class SchemaManagerFactory implements Factory<SchemaManager> {

    private final Optional<SchemaRegistryClient> schemaRegistryClient;
    private final SubjectNameStrategy defaultSubjectNameStrategy;

    @Inject
    private SchemaManagerFactory(
        Optional<SchemaRegistryClient> schemaRegistryClient,
        SubjectNameStrategy defaultSubjectNameStrategy) {
      this.schemaRegistryClient = requireNonNull(schemaRegistryClient);
      this.defaultSubjectNameStrategy = requireNonNull(defaultSubjectNameStrategy);
    }

    @Override
    public SchemaManager provide() {
      if (schemaRegistryClient.isPresent()) {
        return new SchemaManagerImpl(schemaRegistryClient.get(), defaultSubjectNameStrategy);
      } else {
        return new SchemaManagerThrowing();
      }
    }

    @Override
    public void dispose(SchemaManager schemaRecordSerializer) {}
  }
}
