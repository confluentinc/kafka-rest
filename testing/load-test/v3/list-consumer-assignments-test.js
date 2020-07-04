import {randomItem} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {listClusters, listConsumerAssignments, listConsumerGroups, listConsumers} from './common.js'

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
};

export function setup() {
    // TODO: Create topics and consumer on setup (instead of relying on existing topics/consumers),
    //       to make the test more reproducible.

    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let consumers = [];

    let listConsumerGroupsResponse = listConsumerGroups(clusterId);
    listConsumerGroupsResponse.json().data.forEach(
        consumerGroup => {
            let listConsumersResponse = listConsumers(clusterId, consumerGroup.consumer_group_id);
            listConsumersResponse.json().data.forEach(
                consumer => {
                    consumers.push(
                        {
                            consumerGroupId: consumerGroup.consumer_group_id,
                            consumerId: consumer.consumer_id
                        });
                });
        });

    return {clusterId, consumers};
}

export default function (data) {
    let consumer = randomItem(data.consumers);

    listConsumerAssignments(data.clusterId, consumer.consumerGroupId, consumer.consumerId);
}

export function teardown(data) {
}
