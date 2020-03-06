package io.confluent.kafkarest.resources.v3;

import static java.util.Collections.emptyList;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TopicsResourceTest {

  PartitionReplica PARTITION_REPLICA_1 = new PartitionReplica(2, false, true);
  PartitionReplica PARTITION_REPLICA_2 = new PartitionReplica(3, false, true);
  PartitionReplica PARTITION_REPLICA_3 = new PartitionReplica(4, false, true);

  private static Topic TOPIC_1;
  private static Topic TOPIC_2;
  private static Topic TOPIC_3;
  private List<Topic> TOPICLIST = new ArrayList<>();

  private static final Broker BROKER_1 = new Broker(1, "broker-1", 9091, "rack-1");
  private static final Cluster CLUSTER_1 =
      new Cluster("cluster-1", BROKER_1, Arrays.asList());

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicManager topicManager;

  private TopicsResource topicsResource;

  @Before
  public void setUp() {
    topicsResource = new TopicsResource(topicManager, new FakeUrlFactory());
    List<PartitionReplica> partitionsReplica = new ArrayList<>();
    partitionsReplica.add(PARTITION_REPLICA_1);
    partitionsReplica.add(PARTITION_REPLICA_2);
    partitionsReplica.add(PARTITION_REPLICA_3);
    List<Partition> partitions = new ArrayList<>();
    partitions.add(new Partition(0, 0, partitionsReplica));
    TOPIC_1 = new Topic("topic-1", new Properties(),
        partitions, 3, true, "cluster-1");
    TOPIC_2 = new Topic("topic-2", new Properties(),
        partitions, 3, true, "cluster-1");
    TOPIC_3 = new Topic("topic-3", new Properties(),
        partitions, 3, true, "cluster-1");
    TOPICLIST = Arrays.asList(TOPIC_1, TOPIC_2, TOPIC_3);
  }

  @Test
  public void listTopics_withOwnClusterId_returnsTopicsList() {
    expect(topicManager.listTopics(CLUSTER_1.getClusterId()))
        .andReturn(CompletableFuture.completedFuture(TOPICLIST));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_1.getClusterId());

    ListTopicsResponse expected =
        new ListTopicsResponse(
            new CollectionLink("/v3/clusters/cluster-1/topics", null),
            Arrays.asList(
                new TopicData(
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1"),
                    "topic-1",
                    "cluster-1",
                    true,
                    3,
                    new Relationship("/v3/clusters/cluster-1/topics/topic-1/configs"),
                    new Relationship("/v3/clusters/cluster-1/topics/topic-1/partitions")),
                new TopicData(
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-2"),
                    "topic-2",
                    "cluster-1",
                    true,
                    3,
                    new Relationship("/v3/clusters/cluster-1/topics/topic-2/configs"),
                    new Relationship("/v3/clusters/cluster-1/topics/topic-2/partitions")),
                new TopicData(
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-3"),
                    "topic-3",
                    "cluster-1",
                    true,
                    3,
                    new Relationship("/v3/clusters/cluster-1/topics/topic-3/configs"),
                    new Relationship("/v3/clusters/cluster-1/topics/topic-3/partitions"))));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listTopics_timeoutException_returnsTimeoutException() {
    expect(topicManager.listTopics(CLUSTER_1.getClusterId()))
        .andReturn(failedFuture(new TimeoutException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, CLUSTER_1.getClusterId());

    assertEquals(TimeoutException.class, response.getException().getClass());
  }

  @Test
  public void listTopics_differentCluster_returnsEmptyList() {
    expect(topicManager.listTopics("different-cluster"))
        .andReturn(CompletableFuture.completedFuture(emptyList()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.listTopics(response, "different-cluster");

    ListTopicsResponse expected = new ListTopicsResponse(
        new CollectionLink("/v3/clusters/different-cluster/topics", null),
        Arrays.asList());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopics_existingCluster_returnsTopic() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName())).
        andReturn(CompletableFuture.completedFuture(Optional.of(TOPIC_1)));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    GetTopicResponse expected = new GetTopicResponse(
        new TopicData(
            new ResourceLink("/v3/clusters/cluster-1/topics/topic-1"),
            "topic-1",
            "cluster-1",
            true,
            3,
            new Relationship("/v3/clusters/cluster-1/topics/topic-1/configs"),
            new Relationship("/v3/clusters/cluster-1/topics/topic-1/partitions")));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopics_nonexistingCluster_throwsNotFoundException() {
    expect(topicManager.getTopic("different-cluster", TOPIC_1.getName())).
        andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, "different-cluster", TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopics_existingCluster_nonexistingTopic_throwsNotFound() {
    expect(topicManager.getTopic(TOPIC_1.getClusterId(), TOPIC_1.getName()))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicsResource.getTopic(response, TOPIC_1.getClusterId(), TOPIC_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable exception) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(exception);
    return future;
  }
}
