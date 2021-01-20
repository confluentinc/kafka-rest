import {sleep} from "k6";

import {randomItem} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {deleteTopic, listClusters, listTopics} from './common.js';

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
};

export function setup() {
    // TODO: Create topics in setup.

    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;
    let listTopicsResponse = listTopics(clusterId);
    let topics =
        listTopicsResponse
        .json()
        .data
        .filter(topic => topic.topic_name.startsWith('topic-'))
        .map(topic => topic.topic_name);

    return {clusterId, topics};
}

export default function (data) {
    deleteTopic(data.clusterId, randomItem(data.topics));

    sleep(1);
}

export function teardown(data) {
}
