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

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Acl.Operation;
import io.confluent.kafkarest.entities.Acl.PatternType;
import io.confluent.kafkarest.entities.Acl.Permission;
import io.confluent.kafkarest.entities.Acl.ResourceType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;

final class AclManagerImpl implements AclManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  AclManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<Acl>> searchAcls(
      String clusterId,
      Acl.ResourceType resourceType,
      @Nullable String resourceName,
      Acl.PatternType patternType,
      @Nullable String principal,
      @Nullable String host,
      Acl.Operation operation,
      Acl.Permission permission
  ) {
    AclBindingFilter aclBindingFilter =
        new AclBindingFilter(
            new ResourcePatternFilter(
                resourceType.toAdminResourceType(), resourceName, patternType.toAdminPatternType()),
            new AccessControlEntryFilter(
                principal, host, operation.toAclOperation(), permission.toAclPermissionType()));

    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenApply(cluster -> adminClient.describeAcls(aclBindingFilter))
        .thenCompose(
            describeAclsResult -> KafkaFutures.toCompletableFuture(describeAclsResult.values()))
        .thenApply(
            aclBindings ->
                aclBindings.stream()
                    .map(aclBinding -> toAcl(clusterId, aclBinding))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Void> createAcl(
      String clusterId,
      ResourceType resourceType,
      String resourceName,
      PatternType patternType,
      String principal,
      String host,
      Operation operation,
      Permission permission
  ) {
    AclBinding aclBinding =
        new AclBinding(
            new ResourcePattern(
                resourceType.toAdminResourceType(), resourceName, patternType.toAdminPatternType()),
            new AccessControlEntry(
                principal, host, operation.toAclOperation(), permission.toAclPermissionType()));

    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenApply(cluster -> adminClient.createAcls(singletonList(aclBinding)))
        .thenCompose(
            createAclsResult ->
                KafkaFutures.toCompletableFuture(createAclsResult.values().get(aclBinding)));
  }

  @Override
  public CompletableFuture<List<Acl>> deleteAcls(
      String clusterId,
      ResourceType resourceType,
      String resourceName,
      PatternType patternType,
      String principal,
      String host,
      Operation operation,
      Permission permission
  ) {
    AclBindingFilter aclBindingFilter =
        new AclBindingFilter(
            new ResourcePatternFilter(
                resourceType.toAdminResourceType(), resourceName, patternType.toAdminPatternType()),
            new AccessControlEntryFilter(
                principal, host, operation.toAclOperation(), permission.toAclPermissionType()));

    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenApply(cluster -> adminClient.deleteAcls(singletonList(aclBindingFilter)))
        .thenCompose(
            deleteAclsResult ->
                KafkaFutures.toCompletableFuture(deleteAclsResult.values().get(aclBindingFilter)))
        .thenApply(
            filterResults ->
                filterResults.values()
                    .stream()
                    .map(FilterResult::binding)
                    .filter(Objects::nonNull)
                    .map(binding -> toAcl(clusterId, binding))
                    .collect(Collectors.toList()));
  }

  private static Acl toAcl(String clusterId, AclBinding aclBinding) {
    return Acl.fromAclBinding(aclBinding).setClusterId(clusterId).build();
  }
}
