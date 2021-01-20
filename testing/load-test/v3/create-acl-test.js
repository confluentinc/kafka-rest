import {sleep} from "k6";

import {randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {createAcl, deleteAcls, listClusters} from './common.js'

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

    let topics = [];
    for (let i = 0; i < 100; i++) {
        topics.push(`topic-${uuidv4()}`);
    }

    return {
        clusterId: randomItem(listClustersResponse.json().data).cluster_id,
        topics
    }
}

export default function (data) {
    createAcl(
        data.clusterId,
        {
            resource_type: 'TOPIC',
            resource_name: randomItem(data.topics),
            pattern_type: 'LITERAL',
            principal: `User:${uuidv4()}`,
            host: '*',
            operation: 'READ',
            permission: 'ALLOW'
        });

    sleep(1);
}

export function teardown(data) {
    data.topics.forEach(
        topicName => {
            deleteAcls(
                data.clusterId,
                {
                    'resource_type': 'TOPIC',
                    'resource_name': topicName,
                    'pattern_type': 'LITERAL',
                });
        });
}
