import encoding from "k6/encoding";

import {randomIntBetween, randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {produceBinaryToTopic} from "./common.js";
import {createTopic, listClusters} from "../v3/common.js";

export let options = {
    stages: [
        {duration: '1s', target: 100},
        {duration: '240s', target: 100},
        {duration: '1s', target: 0},
    ],
    teardownTimeout: '1m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let topics = [];
    for (let i = 0; i < 10; i++) {
        let topicName = `topic-binary-${uuidv4()}`;
        topics.push(topicName);
        createTopic(clusterId, topicName, 10, 3);
    }

    let message = "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";

    return {clusterId, topics, message};
}

export default function (data) {
    let records = [];
    let size = randomIntBetween(1, 100);

    for (let i = 0; i < size; i++) {
        records.push({
            key: data.message,
            value: data.message
        });
    }

    produceBinaryToTopic(randomItem(data.topics), records);
}

export function teardown(data) {
    // Don't delete topics. Use the messages produced on consumer-binary-test.
}
