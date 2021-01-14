package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.BinaryNode;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.entities.v3.ProduceResponse.ProduceResponseData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class ProduceActionTest  extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public ProduceActionTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ true);

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @Test
  public void foobar() {
    String clusterId = getClusterId();

    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(emptyList())
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf("foo".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf("bar".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse expected =
        ProduceResponse.builder()
            .setClusterId(clusterId)
            .setTopicName(TOPIC_NAME)
            .setPartitionId(0)
            .setOffset(0)
            .setKey(ProduceResponseData.builder().setType(EmbeddedFormat.BINARY).setSize(3).build())
            .setValue(
                ProduceResponseData.builder().setType(EmbeddedFormat.BINARY).setSize(3).build())
            .build();
    ProduceResponse actual = response.readEntity(ProduceResponse.class);
    assertEquals(expected, actual);
  }
}
