import {check} from 'k6';
import http from 'k6/http';
import {Counter, Trend} from 'k6/metrics';

import {randomIntBetween} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

export let randomByteString = function (size) {
    let str = '';
    for (let i = 0; i < size; i++) {
        // First 128 chars (US-ASCII) take 1 byte to UTF-8 encode.
        str += String.fromCharCode(randomIntBetween(0, 128));
    }
    return str;
}

const baseUrl = 'http://localhost:9391';

let produceBinaryToTopicRequestLatency = new Trend('ProduceBinaryToTopicRequestLatency', true);
let produceBinaryToTopicRequestCount = new Counter('ProduceBinaryToTopicRequestCount');
let produceBinaryToTopicMessageCount = new Counter('ProduceBinaryToTopicMessageCount');
let produceBinaryToTopicByteCount = new Counter('ProduceBinaryToTopicByteCount');
export let produceBinaryToTopic = function (topicName, records) {
    const url = `${baseUrl}/topics/${topicName}`;
    const body = {records};
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.binary.v2+json'
    };
    const tags = {name: 'ProduceBinaryToTopic'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'ProduceBinaryToTopic: Status is 200': response => response.status === 200});
    produceBinaryToTopicRequestLatency.add(response.timings.duration);
    produceBinaryToTopicRequestCount.add(1);
    if (response.status === 200) {
        produceBinaryToTopicMessageCount.add(records.length);
        produceBinaryToTopicByteCount.add(
            records.reduce((total, record) => total + record.key.length + record.value.length));
    }
    return response;
}

let produceJsonToTopicRequestLatency = new Trend('ProduceJsonToTopicRequestLatency', true);
let produceJsonToTopicRequestCount = new Counter('ProduceJsonToTopicRequestCount');
let produceJsonToTopicMessageCount = new Counter('ProduceJsonToTopicMessageCount');
let produceJsonToTopicByteCount = new Counter('ProduceJsonToTopicByteCount');
export let produceJsonToTopic = function (topicName, records) {
    const url = `${baseUrl}/topics/${topicName}`;
    const body = {records};
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.json.v2+json'
    };
    const tags = {name: 'ProduceJsonToTopic'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'ProduceJsonToTopic: Status is 200': response => response.status === 200});
    produceJsonToTopicRequestLatency.add(response.timings.duration);
    produceJsonToTopicRequestCount.add(1);
    if (response.status === 200) {
        produceJsonToTopicMessageCount.add(records.length);
        produceJsonToTopicByteCount.add(
            records.reduce(
                (total, record) =>
                    total
                    + JSON.stringify(record.key).length
                    + JSON.stringify(record.value).length));
    }
    return response;
}

let produceBinaryToPartitionRequestLatency = new Trend('ProduceBinaryToPartitionRequestLatency', true);
let produceBinaryToPartitionRequestCount = new Counter('ProduceBinaryToPartitionRequestCount');
let produceBinaryToPartitionMessageCount = new Counter('ProduceBinaryToPartitionMessageCount');
let produceBinaryToPartitionByteCount = new Counter('ProduceBinaryToPartitionByteCount');
export let produceBinaryToPartition = function (topicName, partitionId, records) {
    const url = `${baseUrl}/topics/${topicName}/partitions/${partitionId}`;
    const body = {records};
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.binary.v2+json'
    };
    const tags = {name: 'ProduceBinaryToPartition'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'ProduceBinaryToPartition: Status is 200': response => response.status === 200});
    produceBinaryToPartitionRequestLatency.add(response.timings.duration);
    produceBinaryToPartitionRequestCount.add(1);
    if (response.status === 200) {
        produceBinaryToPartitionMessageCount.add(records.length);
        produceBinaryToPartitionByteCount.add(
            records.reduce((total, record) => total + record.key.length + record.value.length));
    }
    return response;
}

let produceJsonToPartitionRequestLatency = new Trend('ProduceJsonToPartitionRequestLatency', true);
let produceJsonToPartitionRequestCount = new Counter('ProduceJsonToPartitionRequestCount');
let produceJsonToPartitionMessageCount = new Counter('ProduceJsonToPartitionMessageCount');
let produceJsonToPartitionByteCount = new Counter('ProduceJsonToPartitionByteCount');
export let produceJsonToPartition = function (topicName, partitionId, records) {
    const url = `${baseUrl}/topics/${topicName}/partitions/${partitionId}`;
    const body = {records};
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.json.v2+json'
    };
    const tags = {name: 'ProduceJsonToPartition'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'ProduceJsonToPartition: Status is 200': response => response.status === 200});
    produceJsonToPartitionRequestLatency.add(response.timings.duration);
    produceJsonToPartitionRequestCount.add(1);
    if (response.status === 200) {
        produceJsonToPartitionMessageCount.add(records.length);
        produceJsonToPartitionByteCount.add(
            records.reduce(
                (total, record) =>
                    total
                    + JSON.stringify(record.key).length
                    + JSON.stringify(record.value).length));
    }
    return response;
}

let createConsumerRequestLatency = new Trend('CreateConsumerRequestLatency', true);
let createConsumerRequestCount = new Counter('CreateConsumerRequestCount');
export let createConsumer = function (consumerGroupId, consumerId, format) {
    const url = `${baseUrl}/consumers/${consumerGroupId}`;
    const body = {name: consumerId, format, 'auto.offset.reset': 'earliest'};
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.v2+json'
    };
    const tags = {name: 'CreateConsumer'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'CreateConsumer: Status is 200 or 409': response => response.status === 200});
    createConsumerRequestLatency.add(response.timings.duration);
    createConsumerRequestCount.add(1);
    return response;
}

let deleteConsumerRequestLatency = new Trend('DeleteConsumerRequestLatency', true);
let deleteConsumerRequestCount = new Counter('DeleteConsumerRequestCount');
export let deleteConsumer = function (consumerGroupId, consumerId) {
    const url = `${baseUrl}/consumers/${consumerGroupId}/instances/${consumerId}`;
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.v2+json'
    };
    const tags = {name: 'DeleteConsumer'};
    let response = http.del(url, {headers, tags});
    check(response, {'DeleteConsumer: Status is 204': response => response.status === 204});
    deleteConsumerRequestLatency.add(response.timings.duration);
    deleteConsumerRequestCount.add(1);
    return response;
}

let subscribeToTopicPatternRequestLatency = new Trend('SubscribeToTopicPatternRequestLatency', true);
let subscribeToTopicPatternRequestCount = new Counter('SubscribeToTopicPatternRequestCount');
export let subscribeToTopic = function (consumerGroupId, consumerId, topicName) {
    const url = `${baseUrl}/consumers/${consumerGroupId}/instances/${consumerId}/subscription`;
    const body = {'topics': [topicName]};
    const headers = {
        'Accept': 'application/vnd.kafka.v2+json',
        'Content-Type': 'application/vnd.kafka.v2+json'
    };
    const tags = {name: 'SubscribeToTopicPattern'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'SubscribeToTopicPattern: Status is 204': response => response.status === 204});
    subscribeToTopicPatternRequestLatency.add(response.timings.duration);
    subscribeToTopicPatternRequestCount.add(1);
    return response;
}

let listSubscribedTopicsRequestLatency = new Trend('ListSubscribedTopicsRequestLatency', true);
let listSubscribedTopicsRequestCount = new Counter('ListSubscribedTopicsRequestCount');
export let listSubscribedTopics = function (consumerGroupId, consumerId) {
    const url = `${baseUrl}/consumers/${consumerGroupId}/instances/${consumerId}/subscription`;
    const headers = {'Accept': 'application/vnd.kafka.v2+json'};
    const tags = {name: 'ListSubscribedTopics'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListSubscribedTopics: Status is 200': response => response.status === 200});
    listSubscribedTopicsRequestLatency.add(response.timings.duration);
    listSubscribedTopicsRequestCount.add(1);
    return response;
}

let consumeBinaryRequestLatency = new Trend('ConsumeBinaryRequestLatency', true);
let consumeBinaryRequestCount = new Counter('ConsumeBinaryRequestCount');
let consumeBinaryMessagesCount = new Counter('ConsumeBinaryMessagesCount');
let consumeBinaryByteCount = new Counter('ConsumeBinaryByteCount');
export let consumeBinary = function (consumerGroupId, consumerId) {
    const url = `${baseUrl}/consumers/${consumerGroupId}/instances/${consumerId}/records`;
    const headers = {'Accept': 'application/vnd.kafka.binary.v2+json'};
    const tags = {name: 'ConsumeBinary'};
    let response = http.get(url, {headers, tags});
    check(response, {'ConsumeBinary: Status is 200': response => response.status === 200});
    consumeBinaryRequestLatency.add(response.timings.duration);
    consumeBinaryRequestCount.add(1);
    if (response.status === 200) {
        consumeBinaryMessagesCount.add(response.json().length);
        consumeBinaryByteCount.add(
            response.json()
            .reduce((total, record) => total + record.key.length + record.value.length))
    }
    return response;
}

let consumeJsonRequestLatency = new Trend('ConsumeJsonRequestLatency', true);
let consumeJsonRequestCount = new Counter('ConsumeJsonRequestCount');
let consumeJsonMessagesCount = new Counter('ConsumeJsonMessagesCount');
let consumeJsonByteCount = new Counter('ConsumeJsonByteCount');
export let consumeJson = function (consumerGroupId, consumerId) {
    const url = `${baseUrl}/consumers/${consumerGroupId}/instances/${consumerId}/records`;
    const headers = {'Accept': 'application/vnd.kafka.json.v2+json'};
    const tags = {name: 'ConsumeJson'};
    let response = http.get(url, {headers, tags});
    check(response, {'ConsumeJson: Status is 200': response => response.status === 200});
    consumeJsonRequestLatency.add(response.timings.duration);
    consumeJsonRequestCount.add(1);
    if (response.status === 200) {
        consumeJsonMessagesCount.add(response.json().length);
        consumeJsonByteCount.add(
            response.json()
            .reduce(
                (total, record) =>
                    total
                    + JSON.stringify(record.key).length
                    + JSON.stringify(record.value).length))
    }
    return response;
}
