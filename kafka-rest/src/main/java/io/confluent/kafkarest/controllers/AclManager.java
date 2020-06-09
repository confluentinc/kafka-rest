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

package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Acl.Operation;
import io.confluent.kafkarest.entities.Acl.PatternType;
import io.confluent.kafkarest.entities.Acl.Permission;
import io.confluent.kafkarest.entities.Acl.ResourceType;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

public interface AclManager {

  /**
   * Returns the list of Kafka {@link Acl acls} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that match the given search criteria.
   */
  CompletableFuture<List<Acl>> searchAcls(
      String clusterId,
      ResourceType resourceType,
      @Nullable String resourceName,
      PatternType patternType,
      @Nullable String principal,
      @Nullable String host,
      Operation operation,
      Permission permissionType);

  /**
   * Creates a Kafka {@link Acl}.
   */
  CompletableFuture<Void> createAcl(
      String clusterId,
      ResourceType resourceType,
      @Nullable String resourceName,
      PatternType patternType,
      @Nullable String principal,
      @Nullable String host,
      Operation operation,
      Permission permission);

  /**
   * Deletes and returns the list of Kafka {@link Acl acls} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that match the given search criteria.
   */
  CompletableFuture<List<Acl>> deleteAcls(
      String clusterId,
      ResourceType resourceType,
      @Nullable String resourceName,
      PatternType patternType,
      @Nullable String principal,
      @Nullable String host,
      Operation operation,
      Permission permission);
}
