/*
 * Copyright 2022 Confluent Inc.
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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafkarest.controllers.AclManager;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.v3.CreateAclBatchRequest;
import io.confluent.kafkarest.entities.v3.CreateAclBatchRequestData;
import io.confluent.kafkarest.entities.v3.CreateAclRequest;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class CreateAclBatchActionTest {

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

  private static final List<Acl> ACLS = Arrays.asList(ACL_1, ACL_2);

  @Mock public AclManager aclManager;

  private CreateAclBatchAction aclsResource;

  @BeforeEach
  public void setUp() {
    aclsResource = new CreateAclBatchAction(() -> aclManager);
  }

  @Test
  public void createAcls_createsAcls() {
    expect(aclManager.createAcls(CLUSTER_ID, ACLS)).andReturn(completedFuture(/* value= */ null));
    expect(aclManager.validateAclCreateParameters(anyObject())).andReturn(aclManager);
    replay(aclManager);

    CreateAclBatchRequest createRequest =
        CreateAclBatchRequest.create(
            CreateAclBatchRequestData.create(
                ACLS.stream()
                    .map(
                        acl ->
                            CreateAclRequest.builder()
                                .setPrincipal(acl.getPrincipal())
                                .setPermission(acl.getPermission())
                                .setOperation(acl.getOperation())
                                .setHost(acl.getHost())
                                .setPatternType(acl.getPatternType())
                                .setResourceName(acl.getResourceName())
                                .setResourceType(acl.getResourceType())
                                .build())
                    .collect(Collectors.toList())));

    FakeAsyncResponse response = new FakeAsyncResponse();

    aclsResource.createAcls(response, CLUSTER_ID, createRequest);

    assertNull(response.getValue());
    assertNull(response.getException());
  }
}
