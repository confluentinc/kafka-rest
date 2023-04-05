/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Acl.Operation;
import io.confluent.kafkarest.entities.Acl.PatternType;
import io.confluent.kafkarest.entities.Acl.Permission;
import io.confluent.kafkarest.entities.Acl.ResourceType;

@AutoValue
public abstract class AclData extends Resource {

  AclData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("resource_type")
  public abstract ResourceType getResourceType();

  @JsonProperty("resource_name")
  public abstract String getResourceName();

  @JsonProperty("pattern_type")
  public abstract PatternType getPatternType();

  @JsonProperty("principal")
  public abstract String getPrincipal();

  @JsonProperty("host")
  public abstract String getHost();

  @JsonProperty("operation")
  public abstract Operation getOperation();

  @JsonProperty("permission")
  public abstract Permission getPermission();

  public static Builder builder() {
    return new AutoValue_AclData.Builder().setKind("KafkaAcl");
  }

  public static Builder fromAcl(Acl acl) {
    return builder()
        .setClusterId(acl.getClusterId())
        .setResourceType(acl.getResourceType())
        .setResourceName(acl.getResourceName())
        .setPatternType(acl.getPatternType())
        .setPrincipal(acl.getPrincipal())
        .setHost(acl.getHost())
        .setOperation(acl.getOperation())
        .setPermission(acl.getPermission());
  }

  @JsonCreator
  static AclData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("resource_type") ResourceType resourceType,
      @JsonProperty("resource_name") String resourceName,
      @JsonProperty("pattern_type") PatternType patternType,
      @JsonProperty("principal") String principal,
      @JsonProperty("host") String host,
      @JsonProperty("operation") Operation operation,
      @JsonProperty("permission") Permission permission
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setResourceType(resourceType)
        .setResourceName(resourceName)
        .setPatternType(patternType)
        .setPrincipal(principal)
        .setHost(host)
        .setOperation(operation)
        .setPermission(permission)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setResourceType(ResourceType resourceType);

    public abstract Builder setResourceName(String resourceName);

    public abstract Builder setPatternType(PatternType patternType);

    public abstract Builder setPrincipal(String principal);

    public abstract Builder setHost(String host);

    public abstract Builder setOperation(Operation operation);

    public abstract Builder setPermission(Permission permission);

    public abstract AclData build();
  }
}
