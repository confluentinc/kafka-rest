import {randomItem} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {getCluster, listClusters} from './common.js'

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
};

export function setup() {
    let listClustersResponse = listClusters();

    return {clusters: listClustersResponse.json().data.map(cluster => cluster.cluster_id)};
}

export default function (data) {
    getCluster(randomItem(data.clusters));
}

export function teardown(data) {
}
