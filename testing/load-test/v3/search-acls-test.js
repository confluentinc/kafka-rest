import {sleep} from "k6";

import {randomIntBetween, randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {createAcl, deleteAcls, listClusters, searchAcls} from './common.js'

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
    thresholds: {},
    setupTimeout: '1m',
    teardownTimeout: '1m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let topics = [];
    for (let i = 0; i < 10; i++) {
        let topicName = `topic-${uuidv4()}`;
        topics.push(topicName);

        let numAcls = randomIntBetween(1, 100);
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

    return {clusterId, topics}
}

export default function (data) {
    searchAcls(
        data.clusterId,
        {
            'resource_type': 'TOPIC',
            'resource_name': randomItem(data.topics),
            'pattern_type': 'LITERAL',
        });
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
