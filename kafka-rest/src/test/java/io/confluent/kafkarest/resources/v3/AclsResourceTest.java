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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.confluent.kafkarest.controllers.AclManager;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.v3.AclData;
import io.confluent.kafkarest.entities.v3.AclDataList;
import io.confluent.kafkarest.entities.v3.CreateAclRequest;
import io.confluent.kafkarest.entities.v3.DeleteAclsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchAclsResponse;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AclsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final Acl ACL_1 =
      Acl.builder()
          .setClusterId(CLUSTER_ID)
          .setResourceType(Acl.ResourceType.TOPIC)
          .setResourceName("*")
          .setPatternType(Acl.PatternType.LITERAL)
          .setPrincipal("User:alice")
          .setHost("*")
          .setOperation(Acl.Operation.READ)
          .setPermission(Acl.Permission.ALLOW)
          .build();
  private static final Acl ACL_2 =
      Acl.builder()
          .setClusterId(CLUSTER_ID)
          .setResourceType(Acl.ResourceType.TOPIC)
          .setResourceName("topic-")
          .setPatternType(Acl.PatternType.PREFIXED)
          .setPrincipal("User:bob")
          .setHost("1.2.3.4")
          .setOperation(Acl.Operation.WRITE)
          .setPermission(Acl.Permission.ALLOW)
          .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  public AclManager aclManager;

  private AclsResource aclsResource;

  @Before
  public void setUp() {
    aclsResource = new AclsResource(() -> aclManager, new FakeUrlFactory());
  }

  @Test
  public void searchAcls_returnsMatchedAcls() {
    expect(
        aclManager.searchAcls(
            CLUSTER_ID,
            Acl.ResourceType.TOPIC,
            /* resourceName= */ "topic-1",
            Acl.PatternType.MATCH,
            /* principal= */ null,
            /* host= */ null,
            Acl.Operation.ANY,
            Acl.Permission.ALLOW))
        .andReturn(completedFuture(Arrays.asList(ACL_1, ACL_2)));
    replay(aclManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    aclsResource.searchAcls(
        response,
        CLUSTER_ID,
        Acl.ResourceType.TOPIC,
        /* resourceName= */ "topic-1",
        Acl.PatternType.MATCH,
        /* principal= */ "",
        /* host= */ "",
        Acl.Operation.ANY,
        Acl.Permission.ALLOW);

    SearchAclsResponse expected =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/acls"
                                + "?resource_type=TOPIC"
                                + "&resource_name=topic-1"
                                + "&pattern_type=MATCH"
                                + "&principal="
                                + "&host="
                                + "&operation=ANY"
                                + "&permission=ALLOW")
                        .build())
                .setData(
                    Arrays.asList(
                        AclData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/acls"
                                            + "?resource_type=TOPIC"
                                            + "&resource_name=*"
                                            + "&pattern_type=LITERAL"
                                            + "&principal=User:alice"
                                            + "&host=*"
                                            + "&operation=READ"
                                            + "&permission=ALLOW")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setResourceType(Acl.ResourceType.TOPIC)
                            .setResourceName("*")
                            .setPatternType(Acl.PatternType.LITERAL)
                            .setPrincipal("User:alice")
                            .setHost("*")
                            .setOperation(Acl.Operation.READ)
                            .setPermission(Acl.Permission.ALLOW)
                            .build(),
                        AclData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/acls"
                                            + "?resource_type=TOPIC"
                                            + "&resource_name=topic-"
                                            + "&pattern_type=PREFIXED"
                                            + "&principal=User:bob"
                                            + "&host=1.2.3.4"
                                            + "&operation=WRITE"
                                            + "&permission=ALLOW")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setResourceType(Acl.ResourceType.TOPIC)
                            .setResourceName("topic-")
                            .setPatternType(Acl.PatternType.PREFIXED)
                            .setPrincipal("User:bob")
                            .setHost("1.2.3.4")
                            .setOperation(Acl.Operation.WRITE)
                            .setPermission(Acl.Permission.ALLOW)
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void createAcl_createsAcl() {
    expect(
        aclManager.createAcl(
            CLUSTER_ID,
            Acl.ResourceType.TOPIC,
            /* resourceName= */ "*",
            Acl.PatternType.LITERAL,
            /* principal= */ "User:alice",
            /* host= */ "*",
            Acl.Operation.READ,
            Acl.Permission.ALLOW))
        .andReturn(completedFuture(/* value= */ null));
    replay(aclManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    aclsResource.createAcl(
        response,
        CLUSTER_ID,
        CreateAclRequest.builder()
            .setResourceType(Acl.ResourceType.TOPIC)
            .setResourceName("*")
            .setPatternType(Acl.PatternType.LITERAL)
            .setPrincipal("User:alice")
            .setHost("*")
            .setOperation(Acl.Operation.READ)
            .setPermission(Acl.Permission.ALLOW)
            .build());

    assertNull(response.getValue());
    assertNull(response.getException());
  }

  @Test
  public void deleteAcl_deletesAndReturnsMatchedAcls() {
    expect(
        aclManager.deleteAcls(
            CLUSTER_ID,
            Acl.ResourceType.TOPIC,
            /* resourceName= */ "topic-1",
            Acl.PatternType.MATCH,
            /* principal= */ null,
            /* host= */ null,
            Acl.Operation.ANY,
            Acl.Permission.ALLOW))
        .andReturn(completedFuture(Arrays.asList(ACL_1, ACL_2)));
    replay(aclManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    aclsResource.deleteAcls(
        response,
        CLUSTER_ID,
        Acl.ResourceType.TOPIC,
        /* resourceName= */ "topic-1",
        Acl.PatternType.MATCH,
        /* principal= */ "",
        /* host= */ "",
        Acl.Operation.ANY,
        Acl.Permission.ALLOW);

    DeleteAclsResponse expected =
        DeleteAclsResponse.create(
            Arrays.asList(
                AclData.builder()
                    .setMetadata(
                        Resource.Metadata.builder()
                            .setSelf(
                                "/v3/clusters/cluster-1/acls"
                                    + "?resource_type=TOPIC"
                                    + "&resource_name=*"
                                    + "&pattern_type=LITERAL"
                                    + "&principal=User:alice"
                                    + "&host=*"
                                    + "&operation=READ"
                                    + "&permission=ALLOW")
                            .build())
                    .setClusterId(CLUSTER_ID)
                    .setResourceType(Acl.ResourceType.TOPIC)
                    .setResourceName("*")
                    .setPatternType(Acl.PatternType.LITERAL)
                    .setPrincipal("User:alice")
                    .setHost("*")
                    .setOperation(Acl.Operation.READ)
                    .setPermission(Acl.Permission.ALLOW)
                    .build(),
                AclData.builder()
                    .setMetadata(
                        Resource.Metadata.builder()
                            .setSelf(
                                "/v3/clusters/cluster-1/acls"
                                    + "?resource_type=TOPIC"
                                    + "&resource_name=topic-"
                                    + "&pattern_type=PREFIXED"
                                    + "&principal=User:bob"
                                    + "&host=1.2.3.4"
                                    + "&operation=WRITE"
                                    + "&permission=ALLOW")
                            .build())
                    .setClusterId(CLUSTER_ID)
                    .setResourceType(Acl.ResourceType.TOPIC)
                    .setResourceName("topic-")
                    .setPatternType(Acl.PatternType.PREFIXED)
                    .setPrincipal("User:bob")
                    .setHost("1.2.3.4")
                    .setOperation(Acl.Operation.WRITE)
                    .setPermission(Acl.Permission.ALLOW)
                    .build()));

    assertEquals(expected, response.getValue());
  }
}
