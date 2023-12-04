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

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.v3.AclData;
import io.confluent.kafkarest.entities.v3.AclDataList;
import io.confluent.kafkarest.entities.v3.CreateAclBatchRequest;
import io.confluent.kafkarest.entities.v3.CreateAclBatchRequestData;
import io.confluent.kafkarest.entities.v3.CreateAclRequest;
import io.confluent.kafkarest.entities.v3.DeleteAclsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchAclsResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  private String clusterId;
  private String baseAclUrl;
  private String batchAclUrl;
  private String expectedAliceUrl;
  private String expectedBobUrl;
  private String expectedSearchUrl;

  public AclsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @Override
  protected Properties overrideKraftControllerConfig() {
    Properties props = new Properties();
    props.put("authorizer.class.name", StandardAuthorizer.class.getName());
    // this setting allows brokers to register to Kraft controller
    props.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, true);
    return props;
  }

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    if (isKraftTest()) {
      props.put("authorizer.class.name", StandardAuthorizer.class.getName());
    } else {
      props.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
    }
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

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);

    clusterId = getClusterId();
    baseAclUrl = "/v3/clusters/" + clusterId + "/acls";
    batchAclUrl = baseAclUrl + ":batch";
    expectedAliceUrl =
        restConnect
            + baseAclUrl
            + "?resource_type=TOPIC"
            + "&resource_name=*"
            + "&pattern_type=LITERAL"
            + "&principal=User%3Aalice"
            + "&host=*"
            + "&operation=READ"
            + "&permission=ALLOW";
    expectedBobUrl =
        restConnect
            + baseAclUrl
            + "?resource_type=TOPIC"
            + "&resource_name=topic-"
            + "&pattern_type=PREFIXED"
            + "&principal=User%3Abob"
            + "&host=1.2.3.4"
            + "&operation=WRITE"
            + "&permission=ALLOW";
    expectedSearchUrl =
        restConnect
            + baseAclUrl
            + "?resource_type=TOPIC"
            + "&resource_name=topic-1"
            + "&pattern_type=MATCH"
            + "&principal="
            + "&host="
            + "&operation=ANY"
            + "&permission=ANY";
  }

  private void createAliceAndBobAcls() {
    SearchAclsResponse expectedPreCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(emptyList())
                .build());

    Response actualPreCreateSearchResponse =
        request(
                baseAclUrl,
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
        request(baseAclUrl)
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
        request(baseAclUrl)
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
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(
                    Arrays.asList(
                        ALICE_ACL_DATA
                            .setMetadata(
                                Resource.Metadata.builder().setSelf(expectedAliceUrl).build())
                            .setClusterId(clusterId)
                            .build(),
                        BOB_ACL_DATA
                            .setMetadata(
                                Resource.Metadata.builder().setSelf(expectedBobUrl).build())
                            .setClusterId(clusterId)
                            .build()))
                .build());

    Response actualPostCreateSearchResponse =
        request(
                baseAclUrl,
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
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testCreateSearchAndSeparateDelete(String quorum) {
    createAliceAndBobAcls();

    DeleteAclsResponse expectedDeleteAliceResponse =
        DeleteAclsResponse.create(
            singletonList(
                ALICE_ACL_DATA
                    .setMetadata(Resource.Metadata.builder().setSelf(expectedAliceUrl).build())
                    .setClusterId(clusterId)
                    .build()));

    Client webClient = getClient();
    restApp.configureBaseApplication(webClient);
    Response actualDeleteAliceResponse =
        webClient.target(expectedAliceUrl).request().accept(MediaType.APPLICATION_JSON).delete();
    assertEquals(Status.OK.getStatusCode(), actualDeleteAliceResponse.getStatus());
    assertEquals(
        expectedDeleteAliceResponse,
        actualDeleteAliceResponse.readEntity(DeleteAclsResponse.class));

    DeleteAclsResponse expectedDeleteBobResponse =
        DeleteAclsResponse.create(
            singletonList(
                BOB_ACL_DATA
                    .setMetadata(Resource.Metadata.builder().setSelf(expectedBobUrl).build())
                    .setClusterId(clusterId)
                    .build()));

    Response actualDeleteBobResponse =
        webClient.target(expectedBobUrl).request().accept(MediaType.APPLICATION_JSON).delete();
    assertEquals(Status.OK.getStatusCode(), actualDeleteBobResponse.getStatus());
    assertEquals(
        expectedDeleteBobResponse, actualDeleteBobResponse.readEntity(DeleteAclsResponse.class));

    SearchAclsResponse expectedPostDeleteSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(emptyList())
                .build());

    Response actualPostDeleteSearchResponse =
        request(
                baseAclUrl,
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

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testCreateSearchAndMultiDelete(String quorum) {
    createAliceAndBobAcls();

    Client webClient = getClient();
    restApp.configureBaseApplication(webClient);

    // KREST-4113 First ensure that a DELETE request without any parameters specified doesn't
    // delete all ACLs, but throws an HTTP 400 Bad Request instead.
    Response multiDeleteNoParamsResponse =
        webClient
            .target(restConnect + baseAclUrl)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.BAD_REQUEST.getStatusCode(), multiDeleteNoParamsResponse.getStatus());

    DeleteAclsResponse expectedMultiDeleteResponse =
        DeleteAclsResponse.create(
            ImmutableList.of(
                ALICE_ACL_DATA
                    .setMetadata(Resource.Metadata.builder().setSelf(expectedAliceUrl).build())
                    .setClusterId(clusterId)
                    .build(),
                BOB_ACL_DATA
                    .setMetadata(Resource.Metadata.builder().setSelf(expectedBobUrl).build())
                    .setClusterId(clusterId)
                    .build()));
    // Then ensure that a DELETE request with the parameters needed to search for and match both
    // ACLs does delete both ACLs at once.
    Response multiDeleteResourceTypeAll =
        webClient.target(expectedSearchUrl).request().accept(MediaType.APPLICATION_JSON).delete();
    assertEquals(Status.OK.getStatusCode(), multiDeleteResourceTypeAll.getStatus());
    assertEquals(
        expectedMultiDeleteResponse,
        multiDeleteResourceTypeAll.readEntity(DeleteAclsResponse.class));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testMultiDeleteBadQueryParameter(String quorum) {
    createAliceAndBobAcls();

    Client webClient = getClient();
    restApp.configureBaseApplication(webClient);

    Response multiDeleteBadQueryParameterResponse =
        webClient
            .target(restConnect + baseAclUrl)
            .queryParam("resource_type", "teapot")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(
        Status.NOT_FOUND.getStatusCode(), multiDeleteBadQueryParameterResponse.getStatus());

    DeleteAclsResponse expectedMultiDeleteResponse =
        DeleteAclsResponse.create(
            ImmutableList.of(
                ALICE_ACL_DATA
                    .setMetadata(Resource.Metadata.builder().setSelf(expectedAliceUrl).build())
                    .setClusterId(clusterId)
                    .build(),
                BOB_ACL_DATA
                    .setMetadata(Resource.Metadata.builder().setSelf(expectedBobUrl).build())
                    .setClusterId(clusterId)
                    .build()));

    // Then ensure that a DELETE request with the parameters needed to search for and match both
    // ACLs does delete both ACLs at once.
    Response multiDeleteResourceTypeAll =
        webClient.target(expectedSearchUrl).request().accept(MediaType.APPLICATION_JSON).delete();
    assertEquals(Status.OK.getStatusCode(), multiDeleteResourceTypeAll.getStatus());
    assertEquals(
        expectedMultiDeleteResponse,
        multiDeleteResourceTypeAll.readEntity(DeleteAclsResponse.class));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testBatchAclCreate(String quorum) {

    SearchAclsResponse expectedPreCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(emptyList())
                .build());

    Response actualPreCreateSearchResponse =
        request(
                baseAclUrl,
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

    CreateAclRequest bob =
        CreateAclRequest.builder()
            .setResourceType(Acl.ResourceType.TOPIC)
            .setResourceName("topic-")
            .setPatternType(Acl.PatternType.PREFIXED)
            .setPrincipal("User:bob")
            .setHost("1.2.3.4")
            .setOperation(Acl.Operation.WRITE)
            .setPermission(Acl.Permission.ALLOW)
            .build();

    CreateAclRequest alice =
        CreateAclRequest.builder()
            .setResourceType(Acl.ResourceType.TOPIC)
            .setResourceName("*")
            .setPatternType(Acl.PatternType.LITERAL)
            .setPrincipal("User:alice")
            .setHost("*")
            .setOperation(Acl.Operation.READ)
            .setPermission(Acl.Permission.ALLOW)
            .build();

    List<CreateAclRequest> acls = Arrays.asList(bob, alice);

    CreateAclBatchRequestData data = CreateAclBatchRequestData.create(acls);

    CreateAclBatchRequest list = CreateAclBatchRequest.create(data);

    Response actualCreateAliceAndBobResponse =
        request(batchAclUrl).post(Entity.entity(list, MediaType.APPLICATION_JSON));

    assertEquals(Status.NO_CONTENT.getStatusCode(), actualCreateAliceAndBobResponse.getStatus());

    SearchAclsResponse expectedPostCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(
                    Arrays.asList(
                        ALICE_ACL_DATA
                            .setMetadata(
                                Resource.Metadata.builder().setSelf(expectedAliceUrl).build())
                            .setClusterId(clusterId)
                            .build(),
                        BOB_ACL_DATA
                            .setMetadata(
                                Resource.Metadata.builder().setSelf(expectedBobUrl).build())
                            .setClusterId(clusterId)
                            .build()))
                .build());

    Response actualPostCreateSearchResponse =
        request(
                baseAclUrl,
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
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testBatchAclCreateWithNoRequestBody(String quorum) {

    Response nullRequestBodyResponse =
        request(batchAclUrl).post(Entity.entity(null, MediaType.APPLICATION_JSON));

    assertEquals(422, nullRequestBodyResponse.getStatus());

    SearchAclsResponse expectedPostCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(Arrays.asList())
                .build());

    Response actualPostCreateSearchResponse =
        request(
                baseAclUrl,
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
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testBatchAclCreateRequestWithBodyAndNoContent(String quorum) {

    Response emptyRequestBodyResponse =
        request(batchAclUrl).post(Entity.entity("{}}", MediaType.APPLICATION_JSON));

    assertEquals(422, emptyRequestBodyResponse.getStatus());

    SearchAclsResponse expectedPostCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(Arrays.asList())
                .build());

    Response actualPostCreateSearchResponse =
        request(
                baseAclUrl,
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
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testBatchAclCreateWithBodyAndEmptyData(String quorum) {

    List<CreateAclRequest> acls = Arrays.asList();

    CreateAclBatchRequestData data = CreateAclBatchRequestData.create(acls);

    CreateAclBatchRequest list = CreateAclBatchRequest.create(data);

    Response actualCreateAliceAndBobResponse =
        request(batchAclUrl).post(Entity.entity(list, MediaType.APPLICATION_JSON));

    assertEquals(Status.NO_CONTENT.getStatusCode(), actualCreateAliceAndBobResponse.getStatus());

    SearchAclsResponse expectedPostCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(Arrays.asList())
                .build());

    Response actualPostCreateSearchResponse =
        request(
                baseAclUrl,
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
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testBatchAclCreateInvalidEntry(String quorum) {

    CreateAclRequest bob =
        CreateAclRequest.builder()
            .setResourceType(Acl.ResourceType.TOPIC)
            .setResourceName("topic-")
            .setPatternType(Acl.PatternType.PREFIXED)
            .setPrincipal("User:bill")
            .setHost("1.2.3.4")
            .setOperation(Acl.Operation.WRITE)
            .setPermission(Acl.Permission.ALLOW)
            .build();

    CreateAclRequest alice =
        CreateAclRequest.builder()
            .setResourceType(Acl.ResourceType.TOPIC)
            .setResourceName("*")
            .setPatternType(Acl.PatternType.LITERAL)
            .setPrincipal("User:alice")
            .setHost("*")
            .setOperation(Acl.Operation.READ)
            .setPermission(Acl.Permission.ALLOW)
            .build();

    String payload = "{\"data\": [{\"resource_type\":\"TOPIC\"}]}";

    Response actualCreateAliceAndBobResponse =
        request(batchAclUrl).post(Entity.entity(payload, MediaType.APPLICATION_JSON));

    assertEquals(Status.BAD_REQUEST.getStatusCode(), actualCreateAliceAndBobResponse.getStatus());

    SearchAclsResponse expectedPostCreateSearchResponse =
        SearchAclsResponse.create(
            AclDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(expectedSearchUrl).build())
                .setData(Arrays.asList())
                .build());

    Response actualPostCreateSearchResponse =
        request(
                baseAclUrl,
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
  }
}
