import encoding from "k6/encoding";

import {randomIntBetween, randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {produceBinaryToTopic, randomByteString} from "./common.js";
import {createTopic} from "../v3/common.js";

export let options = {
    stages: [
        {duration: '10s', target: 2000},
        {duration: '40s', target: 2000},
        {duration: '10s', target: 0},
    ],
    setupTimeout: '1m',
    teardownTimeout: '1m'
};

export function setup() {
    let topics = [];
    for (let i = 0; i < 10; i++) {
        let topicName = `topic-binary-${uuidv4()}`;
        topics.push(topicName);
        createTopic(clusterId, topicName, 3, 3);
    }

    return {topics};
}

export default function (data) {
    let records = [];
    for (let i = 0; i < randomIntBetween(1, 100); i++) {
        records.push({
            key: encoding.b64encode(randomByteString(randomIntBetween(1, 1025))),
            value: encoding.b64encode(randomByteString(randomIntBetween(1, 4097)))
        });
    }

    produceBinaryToTopic(randomItem(data.topics), records);
}

export function teardown(data) {
    // Don't delete topics. Use the messages produced on consumer-binary-test.
}
