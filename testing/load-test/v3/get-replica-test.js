import {sleep} from "k6";

import {randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {
    createTopic,
    deleteTopic,
    getReplica,
    listClusters,
    listPartitions,
    listReplicas
} from './common.js';

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
    setupTimeout: '3m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let topics = [];
    let replicas = [];

    for (let i = 0; i < 100; i++) {
        let topicName = `topic-${uuidv4()}`;
        topics.push(topicName);

        createTopic(clusterId, topicName, 3, 3);
        sleep(1);

        let listPartitionsResponse = listPartitions(clusterId, topicName);
        listPartitionsResponse.json().data.forEach(
            partition => {
                let listReplicasResponse =
                    listReplicas(clusterId, topicName, partition.partition_id);
                listReplicasResponse.json().data.forEach(
                    replica => {
                        replicas.push({
                            topicName,
                            partitionId: partition.partition_id,
                            brokerId: replica.broker_id
                        });
                    })
            });
    }

    return {clusterId, topics, replicas};
}

export default function (data) {
    let replica = randomItem(data.replicas);

    getReplica(data.clusterId, replica.topicName, replica.partitionId, replica.brokerId);
}

export function teardown(data) {
    data.topics.forEach(topicName => deleteTopic(data.clusterId, topicName));
}
