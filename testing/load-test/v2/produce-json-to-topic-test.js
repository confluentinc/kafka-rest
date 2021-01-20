import {randomIntBetween, randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {produceJsonToTopic} from "./common.js";
import {createTopic, deleteTopic, listClusters} from "../v3/common.js";

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
    teardownTimeout: '1m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let topics = [];
    for (let i = 0; i < 10; i++) {
        let topicName = `topic-${uuidv4()}`;
        topics.push(topicName);
        createTopic(clusterId, topicName, 10, 3);
    }

    return {clusterId, topics};
}

export default function (data) {
    let records = [];
    let size = randomIntBetween(1, 100);
    for (let i = 0; i < size; i++) {
        let message = uuidv4();
        records.push({
            key: {foo: message},
            value: {bar: message}
        });
    }

    produceJsonToTopic(randomItem(data.topics), records);
}

export function teardown(data) {
    data.topics.forEach(topicName => deleteTopic(data.clusterId, topicName));
}
