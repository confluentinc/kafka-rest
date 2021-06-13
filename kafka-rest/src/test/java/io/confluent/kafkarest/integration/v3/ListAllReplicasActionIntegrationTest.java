/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListAllReplicasActionIntegrationTest extends ClusterTestHarness {

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";

    public ListAllReplicasActionIntegrationTest() {
        super(/* numBrokers= */ 2, /* withSchemaRegistry= */ false);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        createTopic(TOPIC_1, 1, (short) 2);
        createTopic(TOPIC_2, 1, (short) 1);
    }

    @Test
    public void listAllReplicas_existingTopics_returnsReplicas() {
        String baseUrl = restConnect;
        String clusterId = getClusterId();

        ResourceCollection.Metadata expectedMetadata =
            ResourceCollection.Metadata.builder()
                .setSelf(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/topics/-/replicas")
                .build();

        TopicDescription topic1Description = describeTopic(TOPIC_1);
        TopicDescription topic2Description = describeTopic(TOPIC_2);

        ReplicaData expectedReplicaData1 =
            createReplicaData(
                baseUrl,
                clusterId,
                TOPIC_1,
                0,
                topic1Description.partitions().get(0).leader().id(),
                true,
                true
                );

        ReplicaData expectedReplicaData2 =
                createReplicaData(
                        baseUrl,
                        clusterId,
                        TOPIC_1,
                        0,
                        topic1Description.partitions().get(0).replicas().stream()
                                .filter(node -> node.id() != topic1Description.partitions().get(0).leader().id())
                                .findFirst().get().id(),
                        false,
                        true
                );

        ReplicaData expectedReplicaData3 =
                createReplicaData(
                        baseUrl,
                        clusterId,
                        TOPIC_2,
                        0,
                        topic2Description.partitions().get(0).leader().id(),
                        true,
                        true
                );

        Response response =
            request("/v3/clusters/" + clusterId + "/topics/-/replicas")
                .accept(MediaType.APPLICATION_JSON)
                .get();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        ListReplicasResponse responseBody = response.readEntity(ListReplicasResponse.class);
        assertEquals(expectedMetadata, responseBody.getValue().getMetadata());
        assertTrue(
            String.format("Not true that `%s' contains `%s'.", responseBody, expectedReplicaData1),
            responseBody.getValue().getData().contains(expectedReplicaData1));
        assertTrue(
            String.format("Not true that `%s' contains `%s'.", responseBody, expectedReplicaData2),
            responseBody.getValue().getData().contains(expectedReplicaData3));
        assertTrue(
            String.format("Not true that `%s' contains `%s'.", responseBody, expectedReplicaData3),
            responseBody.getValue().getData().contains(expectedReplicaData3));
    }

    @Test
    public void listAllReplicas_nonExistingCluster_throwsNotFound() {
        Response response =
            request("/v3/clusters/topics/-/replicas")
                .accept(MediaType.APPLICATION_JSON)
                .get();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    private ReplicaData createReplicaData(
        String baseUrl,
        String clusterId,
        String topicName,
        int partitionId,
        int brokerId,
        boolean isLeader,
        boolean isInSync) {
        return ReplicaData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions/" + partitionId
                            + "/replicas/" + brokerId)
                    .setResourceName(
                        "crn:///kafka="
                            + clusterId + "/topic="
                            + topicName + "/partition=" + partitionId
                            + "/replica=" + brokerId)
                    .build())
            .setBroker(Resource.Relationship.create(baseUrl + "/v3/clusters/" + clusterId
                    + "/brokers/" + brokerId))
            .setClusterId(clusterId)
            .setTopicName(topicName)
            .setLeader(isLeader)
            .setInSync(isInSync)
            .setBrokerId(brokerId)
            .setPartitionId(partitionId)
            .build();
    }
}
