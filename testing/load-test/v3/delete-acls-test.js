import {sleep} from "k6";

import {randomIntBetween, randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {createAcl, deleteAcls, listClusters} from './common.js'

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
    thresholds: {},
    setupTimeout: '10m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let topics = [];
    Array.prototype.push.apply(topics, createAcls(clusterId, 2500, 1, 10));
    Array.prototype.push.apply(topics, createAcls(clusterId, 250, 1, 100));

    return {clusterId, topics}
}

function createAcls(clusterId, numTopics, minAcls, maxAcls) {
    let topics = [];
    for (let i = 0; i < numTopics; i++) {
        let topicName = `topic-${uuidv4()}`;
        topics.push(topicName);

        let numAcls = randomIntBetween(minAcls, maxAcls);
        for (let j = 0; j < numAcls; j++) {
            createAcl(
                clusterId,
                {
                    resource_type: 'TOPIC',
                    resource_name: topicName,
                    pattern_type: 'LITERAL',
                    principal: `User:${uuidv4()}`,
                    host: '*',
                    operation: 'READ',
                    permission: 'ALLOW'
                });
        }
    }
    return topics;
}

export default function (data) {
    deleteAcls(
        data.clusterId,
        {
            'resource_type': 'TOPIC',
            'resource_name': randomItem(data.topics),
            'pattern_type': 'LITERAL',
        });

    sleep(1);
}

export function teardown(data) {
}
