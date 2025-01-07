package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;

import io.confluent.rest.annotations.PerformanceMetric;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.confluent.kafkarest.controllers.PartitionManagerImpl.toTopicPartition;
import static java.util.Objects.requireNonNull;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/offset")
@ResourceName("api.v3.partition-offsets.*")
public class ListPartitionOffsetsAction {

    private final Provider<PartitionManager> partitionManager;


    @Inject
    public ListPartitionOffsetsAction(Provider<PartitionManager> partitionManager) {
        this.partitionManager = requireNonNull(partitionManager);
    }

    @GET
    @PerformanceMetric("v3.partitions.list.offsets")
    @ResourceName("api.v3.partitions.list.offsets")
    public void listPartitions(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("clusterId") String clusterId,
            @PathParam("topicName") String topicName,
            @PathParam("partitionId") Integer partitionId,
            @QueryParam("offset") @DefaultValue("earliest") String offsetType) {


        CompletableFuture<Partition> partitionFuture =
                partitionManager
                        .get()
                        .getPartition(clusterId, topicName, partitionId)
                        .thenApply(partition -> partition.orElseThrow(Errors::partitionNotFoundException));

        CompletableFuture<ListOffsetsResult> listOffsetsFuture = partitionFuture
                .thenApply(partition -> partitionManager.get()
                .listOffsets(Arrays.asList(partition),getOffsetSpecBasedOnType(offsetType)));

        CompletableFuture<ListPartitionOffsetsResponse> response = listOffsetsFuture
                .thenApply(result -> {
                    try {
                        return result.partitionResult(partitionFuture.get().toTopicPartition());
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenApply(listOffsetsResultsInfo -> ListPartitionOffsetsResponse.create(listOffsetsResultsInfo)); //TODO(Apurva): Build response with this.

        AsyncResponses.asyncResume(asyncResponse, response);
    }

    private OffsetSpec getOffsetSpecBasedOnType(String offsetType){
        if ("earliest".equalsIgnoreCase(offsetType)) {
            return OffsetSpec.earliest();
        }
        else if ("latest".equalsIgnoreCase(offsetType))
            return OffsetSpec.latest();
        else {
            throw new IllegalArgumentException("Invalid offset type: " + offsetType);
        }
    }
}
