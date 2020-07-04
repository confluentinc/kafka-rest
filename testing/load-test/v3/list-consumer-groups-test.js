import {randomItem} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {listClusters, listConsumerGroups} from './common.js'

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

    return {clusterId: randomItem(listClustersResponse.json().data).cluster_id};
}

export default function (data) {
    listConsumerGroups(data.clusterId);
}

export function teardown(data) {
}
