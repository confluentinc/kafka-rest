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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Cluster;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AclManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Acl ACL_1 =
      Acl.builder()
          .setClusterId(CLUSTER_ID)
          .setResourceType(Acl.ResourceType.CLUSTER)
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
          .setPermission(Acl.Permission.DENY)
          .build();

  private static final AclBinding ACL_BINDING_1 =
      new AclBinding(
          new ResourcePattern(ResourceType.CLUSTER, /* name= */ "*", PatternType.LITERAL),
          new AccessControlEntry(
              /* principal= */ "User:alice",
              /* host= */ "*",
              AclOperation.READ,
              AclPermissionType.ALLOW));
  private static final AclBinding ACL_BINDING_2 =
      new AclBinding(
          new ResourcePattern(ResourceType.TOPIC, /* name= */ "topic-", PatternType.PREFIXED),
          new AccessControlEntry(
              /* principal= */ "User:bob",
              /* host= */ "1.2.3.4",
              AclOperation.WRITE,
              AclPermissionType.DENY));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private ClusterManager clusterManager;

  @Mock
  private DescribeAclsResult describeAclsResult;

  @Mock
  private CreateAclsResult createAclsResult;

  @Mock
  private DeleteAclsResult deleteAclsResult;

  @Mock
  private FilterResults deleteFilterResults;

  @Mock
  private FilterResult deleteFilterResult1;

  @Mock
  private FilterResult deleteFilterResult2;

  private AclManagerImpl aclManager;

  @Before
  public void setUp() {
    aclManager = new AclManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void searchAcls_returnsMatchedAcls() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(
            completedFuture(
                Optional.of(Cluster.create(CLUSTER_ID, /* controller= */ null, emptyList()))));
    expect(
        adminClient.describeAcls(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.ANY, /* name= */ null, PatternType.ANY),
                new AccessControlEntryFilter(
                    /* principal= */ null,
                    /* host= */ null,
                    AclOperation.ANY,
                    AclPermissionType.ANY))))
        .andReturn(describeAclsResult);
    expect(describeAclsResult.values())
        .andReturn(KafkaFuture.completedFuture(Arrays.asList(ACL_BINDING_1, ACL_BINDING_2)));
    replay(clusterManager, adminClient, describeAclsResult);

    List<Acl> acls =
        aclManager.searchAcls(
            CLUSTER_ID,
            Acl.ResourceType.ANY,
            /* resourceName= */ null,
            Acl.PatternType.ANY,
            /* principal= */ null,
            /* host= */ null,
            Acl.Operation.ANY,
            Acl.Permission.ANY).get();

    assertEquals(Arrays.asList(ACL_1, ACL_2), acls);
  }

  @Test
  public void searchAcls_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      aclManager.searchAcls(
          CLUSTER_ID,
          Acl.ResourceType.ANY,
          /* resourceName= */ null,
          Acl.PatternType.ANY,
          /* principal= */ null,
          /* host= */ null,
          Acl.Operation.ANY,
          Acl.Permission.ANY).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void createAcl_createsAcl() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(
            completedFuture(
                Optional.of(Cluster.create(CLUSTER_ID, /* controller= */ null, emptyList()))));
    expect(adminClient.createAcls(singletonList(ACL_BINDING_1))).andReturn(createAclsResult);
    expect(createAclsResult.values())
        .andReturn(Collections.singletonMap(ACL_BINDING_1, KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, createAclsResult);

    aclManager.createAcl(
        CLUSTER_ID,
        Acl.ResourceType.CLUSTER,
        /* resourceName= */ "*",
        Acl.PatternType.LITERAL,
        /* principal= */ "User:alice",
        /* host= */ "*",
        Acl.Operation.READ,
        Acl.Permission.ALLOW).get();

    verify(adminClient);
  }

  @Test
  public void createAcl_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      aclManager.createAcl(
        CLUSTER_ID,
        Acl.ResourceType.CLUSTER,
        /* resourceName= */ "*",
        Acl.PatternType.LITERAL,
        /* principal= */ "User:alice",
        /* host= */ "*",
        Acl.Operation.READ,
        Acl.Permission.ALLOW).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void deleteAcls_deletesAndReturnsMatchedAcls() throws Exception {
    AclBindingFilter aclBindingFilter =
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, /* name= */ null, PatternType.ANY),
            new AccessControlEntryFilter(
                /* principal= */ null,
                /* host= */ null,
                AclOperation.ANY,
                AclPermissionType.ANY));
    expect(clusterManager.getCluster(CLUSTER_ID))
        .andReturn(
            completedFuture(
                Optional.of(Cluster.create(CLUSTER_ID, /* controller= */ null, emptyList()))));
    expect(adminClient.deleteAcls(singletonList(aclBindingFilter))).andReturn(deleteAclsResult);
    expect(deleteAclsResult.values())
        .andReturn(
            singletonMap(aclBindingFilter, KafkaFuture.completedFuture(deleteFilterResults)));
    expect(deleteFilterResults.values())
        .andReturn(Arrays.asList(deleteFilterResult1, deleteFilterResult2));
    expect(deleteFilterResult1.binding()).andReturn(ACL_BINDING_1);
    expect(deleteFilterResult2.binding()).andReturn(ACL_BINDING_2);
    replay(
        clusterManager,
        adminClient,
        deleteAclsResult,
        deleteFilterResults,
        deleteFilterResult1,
        deleteFilterResult2);

    List<Acl> acls =
        aclManager.deleteAcls(
            CLUSTER_ID,
            Acl.ResourceType.ANY,
            /* resourceName= */ null,
            Acl.PatternType.ANY,
            /* principal= */ null,
            /* host= */ null,
            Acl.Operation.ANY,
            Acl.Permission.ANY).get();

    assertEquals(Arrays.asList(ACL_1, ACL_2), acls);
  }

  @Test
  public void deleteAcls_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      aclManager.deleteAcls(
          CLUSTER_ID,
          Acl.ResourceType.ANY,
          /* resourceName= */ null,
          Acl.PatternType.ANY,
          /* principal= */ null,
          /* host= */ null,
          Acl.Operation.ANY,
          Acl.Permission.ANY).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
