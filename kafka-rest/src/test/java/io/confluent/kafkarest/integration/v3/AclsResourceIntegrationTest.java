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

package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.v3.AclData;
import io.confluent.kafkarest.entities.v3.AclDataList;
import io.confluent.kafkarest.entities.v3.CreateAclRequest;
import io.confluent.kafkarest.entities.v3.DeleteAclsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchAclsResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.Properties;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AclsResourceIntegrationTest extends ClusterTestHarness {

  private static final AclData.Builder ALICE_ACL_DATA =
      AclData.builder()
          .setResourceType(Acl.ResourceType.TOPIC)
          .setResourceName("*")
          .setPatternType(Acl.PatternType.LITERAL)
          .setPrincipal("User:alice")
          .setHost("*")
          .setOperation(Acl.Operation.READ)
          .setPermission(Acl.Permission.ALLOW);

  private static final AclData.Builder BOB_ACL_DATA =
      AclData.builder()
          .setResourceType(Acl.ResourceType.TOPIC)
          .setResourceName("topic-")
          .setPatternType(Acl.PatternType.PREFIXED)
          .setPrincipal("User:bob")
          .setHost("1.2.3.4")
          .setOperation(Acl.Operation.WRITE)
          .setPermission(Acl.Permission.ALLOW);

  public AclsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    props.put("authorizer.class.name", "kafka.security.auth.SimpleAclAuthorizer");
    props.put(
        "listener.name.sasl_plaintext.plain.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"kafka\" "
            + "password=\"kafka\" "
            + "user_kafka=\"kafka\" "
            + "user_kafkarest=\"kafkarest\" "
            + "user_alice=\"alice\" "
            + "user_bob=\"bob\";");
    props.put("sasl.enabled.mechanisms", "PLAIN");
    props.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
    props.put("super.users", "User:kafka;User:kafkarest");
    return props;
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties props) {
    props.put(
        "client.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"kafkarest\" "
            + "password=\"kafkarest\";");
    props.put("client.sasl.mechanism", "PLAIN");
    props.put("client.security.protocol", "SASL_PLAINTEXT");
  }

  @Test
  public void testCreateSearchAndDelete() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    String expectedAliceUrl =
        baseUrl + "/v3/clusters/" + clusterId + "/acls"
            + "?resource_type=TOPIC"
            + "&resource_name=*"
            + "&pattern_type=LITERAL"
            + "&principal=User:alice"
            + "&host=*"
            + "&operation=READ"
            + "&permission=ALLOW";
    String expectedBobUrl =
        baseUrl + "/v3/clusters/" + clusterId + "/acls"
            + "?resource_type=TOPIC"
            + "&resource_name=topic-"
            + "&pattern_type=PREFIXED"
            + "&principal=User:bob"
            + "&host=1.2.3.4"
            + "&operation=WRITE"
            + "&permission=ALLOW";
    String expectedSearchUrl =
        baseUrl + "/v3/clusters/" + clusterId + "/acls"
            + "?resource_type=TOPIC"
            + "&resource_name=topic-1"
            + "&pattern_type=MATCH"
            + "&principal="
            + "&host="
            + "&operation=ANY"
            + "&permission=ANY";

    SearchAclsResponse expectedPreCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(expectedSearchUrl)
                        .build())
                .setData(emptyList())
                .build());

    Response actualPreCreateSearchResponse =
        request(
            "/v3/clusters/" + clusterId + "/acls",
            ImmutableMap.of(
                "resource_type", "topic",
                "resource_name", "topic-1",
                "pattern_type", "match"))
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), actualPreCreateSearchResponse.getStatus());
    assertEquals(
        expectedPreCreateSearchResponse,
        actualPreCreateSearchResponse.readEntity(SearchAclsResponse.class));

    Response actualCreateAliceResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .post(
                Entity.entity(
                    CreateAclRequest.builder()
                        .setResourceType(Acl.ResourceType.TOPIC)
                        .setResourceName("*")
                        .setPatternType(Acl.PatternType.LITERAL)
                        .setPrincipal("User:alice")
                        .setHost("*")
                        .setOperation(Acl.Operation.READ)
                        .setPermission(Acl.Permission.ALLOW)
                        .build(),
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), actualCreateAliceResponse.getStatus());
    assertEquals(expectedAliceUrl, actualCreateAliceResponse.getLocation().toString());

    Response actualCreateBobResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .post(
                Entity.entity(
                    CreateAclRequest.builder()
                        .setResourceType(Acl.ResourceType.TOPIC)
                        .setResourceName("topic-")
                        .setPatternType(Acl.PatternType.PREFIXED)
                        .setPrincipal("User:bob")
                        .setHost("1.2.3.4")
                        .setOperation(Acl.Operation.WRITE)
                        .setPermission(Acl.Permission.ALLOW)
                        .build(),
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), actualCreateBobResponse.getStatus());
    assertEquals(expectedBobUrl, actualCreateBobResponse.getLocation().toString());

    SearchAclsResponse expectedPostCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(expectedSearchUrl)
                        .build())
                .setData(
                    Arrays.asList(
                        ALICE_ACL_DATA.setMetadata(
                            Resource.Metadata.builder()
                                .setSelf(expectedAliceUrl)
                                .build())
                            .setClusterId(clusterId)
                            .build(),
                        BOB_ACL_DATA.setMetadata(
                            Resource.Metadata.builder()
                                .setSelf(expectedBobUrl)
                                .build())
                            .setClusterId(clusterId)
                            .build()))
                .build());

    Response actualPostCreateSearchResponse =
        request(
            "/v3/clusters/" + clusterId + "/acls",
            ImmutableMap.of(
                "resource_type", "topic",
                "resource_name", "topic-1",
                "pattern_type", "match"))
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), actualPostCreateSearchResponse.getStatus());
    assertEquals(
        expectedPostCreateSearchResponse,
        actualPostCreateSearchResponse.readEntity(SearchAclsResponse.class));

    DeleteAclsResponse expectedDeleteAliceResponse =
        DeleteAclsResponse.create(
            singletonList(
                ALICE_ACL_DATA.setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(expectedAliceUrl)
                        .build())
                    .setClusterId(clusterId)
                    .build()));

    Client webClient = getClient();
    restApp.configureBaseApplication(webClient);
    Response actualDeleteAliceResponse =
        webClient.target(actualCreateAliceResponse.getLocation())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.OK.getStatusCode(), actualDeleteAliceResponse.getStatus());
    assertEquals(
        expectedDeleteAliceResponse,
        actualDeleteAliceResponse.readEntity(DeleteAclsResponse.class));

    DeleteAclsResponse expectedDeleteBobResponse =
        DeleteAclsResponse.create(
            singletonList(
                BOB_ACL_DATA.setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(expectedBobUrl)
                        .build())
                    .setClusterId(clusterId)
                    .build()));

    Response actualDeleteBobResponse =
        webClient.target(actualCreateBobResponse.getLocation())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.OK.getStatusCode(), actualDeleteBobResponse.getStatus());
    assertEquals(
        expectedDeleteBobResponse,
        actualDeleteBobResponse.readEntity(DeleteAclsResponse.class));

    SearchAclsResponse expectedPostDeleteSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(expectedSearchUrl)
                        .build())
                .setData(emptyList())
                .build());

    Response actualPostDeleteSearchResponse =
        request(
            "/v3/clusters/" + clusterId + "/acls",
            ImmutableMap.of(
                "resource_type", "topic",
                "resource_name", "topic-1",
                "pattern_type", "match"))
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), actualPostDeleteSearchResponse.getStatus());
    assertEquals(
        expectedPostDeleteSearchResponse,
        actualPostDeleteSearchResponse.readEntity(SearchAclsResponse.class));
  }
}
