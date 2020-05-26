package io.confluent.kafkarest.entities;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

// maybe TopicPartitionReassignment or Reassignments
public final class Reassignment {

  private final String clusterId;

  private final String topicName;

  private final int partitionId;

  private final List<Integer> replicas;

  private final List<Integer> addingReplicas;

  private final List<Integer> removingReplicas;

  public Reassignment(
      String clusterId,
      String topicName,
      int partitionId,
      List<Integer> replicas,
      List<Integer> addingReplicas,
      List<Integer> removingReplicas
      ) {
    this.clusterId = clusterId;
    this.topicName = topicName;
    this.partitionId = partitionId;
    this.replicas = replicas;
    this.addingReplicas = addingReplicas;
    this.removingReplicas = removingReplicas;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getTopicName() {
    return topicName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public List<Integer> getReplicas() {
    return replicas;
  }

  public List<Integer> getAddingReplicas() {
    return addingReplicas;
  }

  public List<Integer> getRemovingReplicas() {
    return removingReplicas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Reassignment that = (Reassignment) o;
    return partitionId == that.partitionId &&
        clusterId.equals(that.clusterId) &&
        topicName.equals(that.topicName) &&
        replicas.equals(that.replicas) &&
        addingReplicas.equals(that.addingReplicas) &&
        removingReplicas.equals(that.removingReplicas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, topicName, partitionId, replicas, addingReplicas, removingReplicas);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Reassignment.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("topicName='" + topicName + "'")
        .add("partitionId=" + partitionId)
        .add("replicas=" + replicas)
        .add("addingReplicas=" + addingReplicas)
        .add("removingReplicas=" + removingReplicas)
        .toString();
  }
}
