import {randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {consumeBinary, createConsumer, subscribeToTopic} from "./common.js";
import {listClusters, listTopics} from "../v3/common.js";

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '30s', target: 100},
        {duration: '10s', target: 0},
    ],
    setupTimeout: '10m',
    teardownTimeout: '1m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    // Use the topics/messages from produce-binary-to-topic-test.
    let listTopicsResponse = listTopics(clusterId);
    let topics =
        listTopicsResponse.json().data
        .filter(topic => topic.topic_name.startsWith('topic-binary-'))
        .map(topic => topic.topic_name)
        .slice(0, 10);

    let consumers = [];
    topics.forEach(
        topicName => {
            let consumerGroupId = `consumer-group-${uuidv4()}`;
            for (let i = 0; i < 10; i++) {
                let consumerId = `consumer-${uuidv4()}`;
                consumers.push({consumerGroupId, consumerId});
                createConsumer(consumerGroupId, consumerId, 'BINARY');
                subscribeToTopic(consumerGroupId, consumerId, topicName);
            }
        });

    return {clusterId, consumers, topics};
}

export default function (data) {
    let consumer = data.consumers[__VU % data.consumers.length];
    consumeBinary(consumer.consumerGroupId, consumer.consumerId);
}

export function teardown(data) {
    // Shutting down the consumers and deleting the topics doesn't work very well. Just restart the
    // server.
}
