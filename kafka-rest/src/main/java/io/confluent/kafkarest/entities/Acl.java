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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

@AutoValue
public abstract class Acl {

  Acl() {
  }

  public abstract String getClusterId();

  public abstract ResourceType getResourceType();

  public abstract String getResourceName();

  public abstract PatternType getPatternType();

  public abstract String getPrincipal();

  public abstract String getHost();

  public abstract Operation getOperation();

  public abstract Permission getPermission();

  public static Builder builder() {
    return new AutoValue_Acl.Builder();
  }

  public static Builder fromAclBinding(AclBinding acl) {
    return builder()
        .setResourceType(ResourceType.fromAdminResourceType(acl.pattern().resourceType()))
        .setResourceName(acl.pattern().name())
        .setPatternType(PatternType.fromAdminPatternType(acl.pattern().patternType()))
        .setPrincipal(acl.entry().principal())
        .setHost(acl.entry().host())
        .setOperation(Operation.fromAclOperation(acl.entry().operation()))
        .setPermission(Permission.fromAclPermissionType(acl.entry().permissionType()));
  }

  @AutoValue.Builder
  public abstract static class Builder {

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

    public abstract Acl build();
  }

  public enum Operation {

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#UNKNOWN}.
     */
    UNKNOWN,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#ANY}.
     */
    ANY,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#ALL}.
     */
    ALL,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#READ}.
     */
    READ,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#WRITE}.
     */
    WRITE,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#CREATE}.
     */
    CREATE,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#DELETE}.
     */
    DELETE,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#ALTER}.
     */
    ALTER,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#DESCRIBE}.
     */
    DESCRIBE,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#CLUSTER_ACTION}.
     */
    CLUSTER_ACTION,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#DESCRIBE_CONFIGS}.
     */
    DESCRIBE_CONFIGS,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#ALTER_CONFIGS}.
     */
    ALTER_CONFIGS,

    /**
     * See {@link org.apache.kafka.common.acl.AclOperation#IDEMPOTENT_WRITE}.
     */
    IDEMPOTENT_WRITE;

    public static Operation fromAclOperation(AclOperation aclOperation) {
      try {
        return valueOf(aclOperation.name());
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    public AclOperation toAclOperation() {
      try {
        return AclOperation.valueOf(name());
      } catch (IllegalArgumentException e) {
        return AclOperation.UNKNOWN;
      }
    }
  }

  public enum PatternType {

    /**
     * See {@link org.apache.kafka.common.resource.PatternType#UNKNOWN}.
     */
    UNKNOWN,

    /**
     * See {@link org.apache.kafka.common.resource.PatternType#ANY}.
     */
    ANY,

    /**
     * See {@link org.apache.kafka.common.resource.PatternType#MATCH}.
     */
    MATCH,

    /**
     * See {@link org.apache.kafka.common.resource.PatternType#LITERAL}.
     */
    LITERAL,

    /**
     * See {@link org.apache.kafka.common.resource.PatternType#PREFIXED}.
     */
    PREFIXED;

    public static PatternType fromAdminPatternType(
        org.apache.kafka.common.resource.PatternType patternType) {
      try {
        return valueOf(patternType.name());
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    public org.apache.kafka.common.resource.PatternType toAdminPatternType() {
      try {
        return org.apache.kafka.common.resource.PatternType.valueOf(name());
      } catch (IllegalArgumentException e) {
        return org.apache.kafka.common.resource.PatternType.UNKNOWN;
      }
    }
  }

  public enum Permission {

    /**
     * See {@link org.apache.kafka.common.acl.AclPermissionType#UNKNOWN}.
     */
    UNKNOWN,

    /**
     * See {@link org.apache.kafka.common.acl.AclPermissionType#ANY}.
     */
    ANY,

    /**
     * See {@link org.apache.kafka.common.acl.AclPermissionType#DENY}.
     */
    DENY,

    /**
     * See {@link org.apache.kafka.common.acl.AclPermissionType#ALLOW}.
     */
    ALLOW;

    public static Permission fromAclPermissionType(AclPermissionType aclPermissionType) {
      try {
        return valueOf(aclPermissionType.name());
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    public AclPermissionType toAclPermissionType() {
      try {
        return AclPermissionType.valueOf(name());
      } catch (IllegalArgumentException e) {
        return AclPermissionType.UNKNOWN;
      }
    }
  }

  public enum ResourceType {

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#UNKNOWN}.
     */
    UNKNOWN,

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#ANY}.
     */
    ANY,

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#TOPIC}.
     */
    TOPIC,

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#GROUP}.
     */
    GROUP,

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#CLUSTER}.
     */
    CLUSTER,

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#TRANSACTIONAL_ID}.
     */
    TRANSACTIONAL_ID,

    /**
     * See {@link org.apache.kafka.common.resource.ResourceType#DELEGATION_TOKEN}.
     */
    DELEGATION_TOKEN;

    public static ResourceType fromAdminResourceType(
        org.apache.kafka.common.resource.ResourceType resourceType) {
      try {
        return valueOf(resourceType.name());
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    public org.apache.kafka.common.resource.ResourceType toAdminResourceType() {
      try {
        return org.apache.kafka.common.resource.ResourceType.valueOf(name());
      } catch (IllegalArgumentException e) {
        return org.apache.kafka.common.resource.ResourceType.UNKNOWN;
      }
    }
  }
}
