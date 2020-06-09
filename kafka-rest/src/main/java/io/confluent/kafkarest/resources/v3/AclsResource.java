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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.controllers.AclManager;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Acl.Operation;
import io.confluent.kafkarest.entities.Acl.PatternType;
import io.confluent.kafkarest.entities.Acl.Permission;
import io.confluent.kafkarest.entities.Acl.ResourceType;
import io.confluent.kafkarest.entities.v3.AclData;
import io.confluent.kafkarest.entities.v3.AclDataList;
import io.confluent.kafkarest.entities.v3.CreateAclRequest;
import io.confluent.kafkarest.entities.v3.DeleteAclsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchAclsResponse;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.UrlFactory;
import java.net.URI;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/acls")
public final class AclsResource {

  private final Provider<AclManager> aclManager;
  private final UrlFactory urlFactory;

  @Inject
  public AclsResource(Provider<AclManager> aclManager, UrlFactory urlFactory) {
    this.aclManager = requireNonNull(aclManager);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void searchAcls(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @QueryParam("resource_type") @DefaultValue("any") ResourceType resourceType,
      @QueryParam("resource_name") @DefaultValue("") String resourceName,
      @QueryParam("pattern_type") @DefaultValue("any") PatternType patternType,
      @QueryParam("principal") @DefaultValue("") String principal,
      @QueryParam("host") @DefaultValue("") String host,
      @QueryParam("operation") @DefaultValue("any") Operation operation,
      @QueryParam("permission") @DefaultValue("any") Permission permission
  ) {
    if (resourceType == Acl.ResourceType.UNKNOWN) {
      throw new BadRequestException("resource_type cannot be ANY");
    }
    if (patternType == Acl.PatternType.UNKNOWN) {
      throw new BadRequestException("pattern_type cannot be UNKNOWN");
    }
    if (operation == Acl.Operation.UNKNOWN) {
      throw new BadRequestException("operation cannot be ANY");
    }
    if (permission == Acl.Permission.UNKNOWN) {
      throw new BadRequestException("permission cannot be ANY");
    }

    CompletableFuture<SearchAclsResponse> response =
        aclManager.get()
            .searchAcls(
                clusterId,
                resourceType,
                resourceName.isEmpty() ? null : resourceName,
                patternType,
                principal.isEmpty() ? null : principal,
                host.isEmpty() ? null : host,
                operation,
                permission)
            .thenApply(
                acls ->
                    SearchAclsResponse.create(
                        AclDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.newUrlBuilder()
                                            .appendPathSegment("v3")
                                            .appendPathSegment("clusters")
                                            .appendPathSegment(clusterId)
                                            .appendPathSegment("acls")
                                            .putQueryParameter("resource_type", resourceType.name())
                                            .putQueryParameter("resource_name", resourceName)
                                            .putQueryParameter("pattern_type", patternType.name())
                                            .putQueryParameter("principal", principal)
                                            .putQueryParameter("host", host)
                                            .putQueryParameter("operation", operation.name())
                                            .putQueryParameter("permission", permission.name())
                                            .build())
                                    .build())
                            .setData(
                                acls.stream()
                                    .map(this::toAclData)
                                    .sorted(
                                        Comparator.comparing(AclData::getResourceType)
                                            .thenComparing(AclData::getResourceName)
                                            .thenComparing(AclData::getPatternType)
                                            .thenComparing(AclData::getPrincipal)
                                            .thenComparing(AclData::getHost)
                                            .thenComparing(AclData::getOperation)
                                            .thenComparing(AclData::getPermission))
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void createAcl(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @Valid CreateAclRequest request
  ) {
    if (request.getResourceType() == Acl.ResourceType.ANY
        || request.getResourceType() == Acl.ResourceType.UNKNOWN) {
      throw new BadRequestException("resource_type cannot be ANY");
    }
    if (request.getPatternType() == Acl.PatternType.ANY
        || request.getPatternType() == Acl.PatternType.MATCH
        || request.getPatternType() == Acl.PatternType.UNKNOWN) {
      throw new BadRequestException(
          String.format("pattern_type cannot be %s", request.getPatternType()));
    }
    if (request.getOperation() == Acl.Operation.ANY
        || request.getOperation() == Acl.Operation.UNKNOWN) {
      throw new BadRequestException("operation cannot be ANY");
    }
    if (request.getPermission() == Acl.Permission.ANY
        || request.getPermission() == Acl.Permission.UNKNOWN) {
      throw new BadRequestException("permission cannot be ANY");
    }

    CompletableFuture<Void> response =
        aclManager.get()
            .createAcl(
                clusterId,
                request.getResourceType(),
                request.getResourceName(),
                request.getPatternType(),
                request.getPrincipal(),
                request.getHost(),
                request.getOperation(),
                request.getPermission());

    AsyncResponseBuilder.from(
        Response.status(Status.CREATED)
            .location(
                URI.create(
                    urlFactory.newUrlBuilder()
                        .appendPathSegment("v3")
                        .appendPathSegment("clusters")
                        .appendPathSegment(clusterId)
                        .appendPathSegment("acls")
                        .putQueryParameter("resource_type", request.getResourceType().name())
                        .putQueryParameter("resource_name", request.getResourceName())
                        .putQueryParameter("pattern_type", request.getPatternType().name())
                        .putQueryParameter("principal", request.getPrincipal())
                        .putQueryParameter("host", request.getHost())
                        .putQueryParameter("operation", request.getOperation().name())
                        .putQueryParameter("permission", request.getPermission().name())
                        .build())))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteAcls(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @QueryParam("resource_type") @DefaultValue("any") ResourceType resourceType,
      @QueryParam("resource_name") @DefaultValue("") String resourceName,
      @QueryParam("pattern_type") @DefaultValue("any") PatternType patternType,
      @QueryParam("principal") @DefaultValue("") String principal,
      @QueryParam("host") @DefaultValue("") String host,
      @QueryParam("operation") @DefaultValue("any") Operation operation,
      @QueryParam("permission") @DefaultValue("any") Permission permission
  ) {
    if (resourceType == Acl.ResourceType.UNKNOWN) {
      throw new BadRequestException("resource_type cannot be ANY");
    }
    if (patternType == Acl.PatternType.UNKNOWN) {
      throw new BadRequestException("pattern_type cannot be UNKNOWN");
    }
    if (operation == Acl.Operation.UNKNOWN) {
      throw new BadRequestException("operation cannot be ANY");
    }
    if (permission == Acl.Permission.UNKNOWN) {
      throw new BadRequestException("permission cannot be ANY");
    }

    CompletableFuture<DeleteAclsResponse> response =
        aclManager.get()
            .deleteAcls(
                clusterId,
                resourceType,
                resourceName.isEmpty() ? null : resourceName,
                patternType,
                principal.isEmpty() ? null : principal,
                host.isEmpty() ? null : host,
                operation,
                permission)
            .thenApply(
                acls ->
                    DeleteAclsResponse.create(
                        acls.stream()
                            .map(this::toAclData)
                            .sorted(
                                Comparator.comparing(AclData::getResourceType)
                                    .thenComparing(AclData::getResourceName)
                                    .thenComparing(AclData::getPatternType)
                                    .thenComparing(AclData::getPrincipal)
                                    .thenComparing(AclData::getHost)
                                    .thenComparing(AclData::getOperation)
                                    .thenComparing(AclData::getPermission))
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  public AclData toAclData(Acl acl) {
    return AclData.fromAcl(acl)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.newUrlBuilder()
                        .appendPathSegment("v3")
                        .appendPathSegment("clusters")
                        .appendPathSegment(acl.getClusterId())
                        .appendPathSegment("acls")
                        .putQueryParameter("resource_type", acl.getResourceType().name())
                        .putQueryParameter("resource_name", acl.getResourceName())
                        .putQueryParameter("pattern_type", acl.getPatternType().name())
                        .putQueryParameter("principal", acl.getPrincipal())
                        .putQueryParameter("host", acl.getHost())
                        .putQueryParameter("operation", acl.getOperation().name())
                        .putQueryParameter("permission", acl.getPermission().name())
                        .build())
                .build())
        .build();
  }
}
