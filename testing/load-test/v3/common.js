import {check} from 'k6';
import http from 'k6/http';
import {Counter, Trend} from 'k6/metrics';

import formurlencode from "https://jslib.k6.io/form-urlencoded/3.0.0/index.js";

let baseThresholds = {};

export let thresholds = function (thresholds) {
    return Object.assign(Object.assign({}, baseThresholds), thresholds);
}

const baseUrl = 'http://localhost:9391';

let listClustersRequestLatency = new Trend('ListClustersRequestLatency', true);
let listClustersRequestCount = new Counter('ListClustersRequestCount');
export let listClusters = function () {
    const url = `${baseUrl}/v3/clusters`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListClusters'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListClusters: Status is 200': response => response.status === 200});
    listClustersRequestLatency.add(response.timings.duration);
    listClustersRequestCount.add(1);
    return response;
}

let getClusterRequestLatency = new Trend('GetClusterRequestLatency', true);
let getClusterRequestCount = new Counter('GetClusterRequestCount');
export let getCluster = function (clusterId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetCluster'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetCluster: Status is 200': response => response.status === 200});
    getClusterRequestLatency.add(response.timings.duration);
    getClusterRequestCount.add(1);
    return response;
}

let searchAclsRequestLatency = new Trend('SearchAclsRequestLatency', true);
let searchAclsRequestCount = new Counter('SearchAclsRequestCount');
export let searchAcls = function (clusterId, params) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/acls?${formurlencode(params)}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'SearchAcls'};
    let response = http.get(url, {headers, tags});
    check(response, {'SearchAcls: Status is 200': response => response.status === 200});
    searchAclsRequestLatency.add(response.timings.duration);
    searchAclsRequestCount.add(1);
    return response;
}

let createAclsRequestLatency = new Trend('CreateAclRequestLatency', true);
let createAclsRequestCount = new Counter('CreateAclRequestCount');
export let createAcl = function (clusterId, params) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/acls`;
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'CreateAcl'};
    let response = http.post(url, JSON.stringify(params), {headers, tags});
    check(response, {'CreateAcl: Status is 201': response => response.status === 201});
    createAclsRequestLatency.add(response.timings.duration);
    createAclsRequestCount.add(1);
    return response;
}

let deleteAclsRequestLatency = new Trend('DeleteAclsRequestLatency', true);
let deleteAclsRequestCount = new Counter('DeleteAclsRequestCount');
export let deleteAcls = function (clusterId, params) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/acls?${formurlencode(params)}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'DeleteAcls'};
    let response = http.del(url, {headers, tags});
    check(response, {'DeleteAcls: Status is 200': response => response.status === 200});
    deleteAclsRequestLatency.add(response.timings.duration);
    deleteAclsRequestCount.add(1);
    return response;
}

let listClusterConfigsRequestLatency = new Trend('ListClusterConfigsRequestLatency', true);
let listClusterConfigsRequestCount = new Counter('ListClusterConfigsRequestCount');
export let listClusterConfigs = function (clusterId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/broker-configs`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListClusterConfigs'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListClusterConfigs: Status is 200': response => response.status === 200});
    listClusterConfigsRequestLatency.add(response.timings.duration);
    listClusterConfigsRequestCount.add(1);
    return response;
}

let alterClusterConfigsRequestLatency = new Trend('AlterClusterConfigsRequestLatency', true);
let alterClusterConfigsRequestCount = new Counter('AlterClusterConfigsRequestCount');
export let alterClusterConfigs = function (clusterId, configs) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/broker-configs:alter`;
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'AlterClusterConfigs'};
    let response = http.post(url, JSON.stringify(configs), {headers, tags});
    check(response, {'AlterClusterConfigs: Status is 204': response => response.status === 204});
    alterClusterConfigsRequestLatency.add(response.timings.duration);
    alterClusterConfigsRequestCount.add(1);
    return response;
}

let getClusterConfigRequestLatency = new Trend('GetClusterConfigRequestLatency', true);
let getClusterConfigRequestCount = new Counter('GetClusterConfigRequestCount');
export let getClusterConfig = function (clusterId, configName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/broker-configs/${configName}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetClusterConfig'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetClusterConfig: Status is 200': response => response.status === 200});
    getClusterConfigRequestLatency.add(response.timings.duration);
    getClusterConfigRequestCount.add(1);
    return response;
}

let updateClusterConfigRequestLatency = new Trend('UpdateClusterConfigRequestLatency', true);
let updateClusterConfigRequestCount = new Counter('UpdateClusterConfigRequestCount');
export let updateClusterConfig = function (clusterId, configName, newValue) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/broker-configs/${configName}`;
    const body = {value: newValue};
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'UpdateClusterConfig'};
    let response = http.put(url, JSON.stringify(body), {headers, tags});
    check(response, {'UpdateClusterConfig: Status is 204': response => response.status === 204});
    updateClusterConfigRequestLatency.add(response.timings.duration);
    updateClusterConfigRequestCount.add(1);
    return response;
}

let resetClusterConfigRequestLatency = new Trend('ResetClusterConfigRequestLatency', true);
let resetClusterConfigRequestCount = new Counter('ResetClusterConfigRequestCount');
export let resetClusterConfig = function (clusterId, configName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/broker-configs/${configName}`;
    const headers = {};
    const tags = {name: 'ResetClusterConfig'};
    let response = http.del(url, {headers, tags});
    check(response, {'ResetClusterConfig: Status is 204': response => response.status === 204});
    resetClusterConfigRequestLatency.add(response.timings.duration);
    resetClusterConfigRequestCount.add(1);
    return response;
}

let listBrokersRequestLatency = new Trend('ListBrokersRequestLatency', true);
let listBrokersRequestCount = new Counter('ListBrokersRequestCount');
export let listBrokers = function (clusterId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListBrokers'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListBrokers: Status is 200': response => response.status === 200});
    listBrokersRequestLatency.add(response.timings.duration);
    listBrokersRequestCount.add(1);
    return response;
}

let getBrokerRequestLatency = new Trend('GetBrokerRequestLatency', true);
let getBrokerRequestCount = new Counter('GetBrokerRequestCount');
export let getBroker = function (clusterId, brokerId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetBroker'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetBroker: Status is 200': response => response.status === 200});
    getBrokerRequestLatency.add(response.timings.duration);
    getBrokerRequestCount.add(1);
    return response;
}

let listBrokerConfigsRequestLatency = new Trend('ListBrokerConfigsRequestLatency', true);
let listBrokerConfigsRequestCount = new Counter('ListBrokerConfigsRequestCount');
export let listBrokerConfigs = function (clusterId, brokerId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}/configs`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListBrokerConfigs'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListBrokerConfigs: Status is 200': response => response.status === 200});
    listBrokerConfigsRequestLatency.add(response.timings.duration);
    listBrokerConfigsRequestCount.add(1);
    return response;
}

let alterBrokerConfigsRequestLatency = new Trend('AlterBrokerConfigsRequestLatency', true);
let alterBrokerConfigsRequestCount = new Counter('AlterBrokerConfigsRequestCount');
export let alterBrokerConfigs = function (clusterId, brokerId, configs) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}/configs:alter`;
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'AlterBrokerConfigs'};
    let response = http.post(url, JSON.stringify(configs), {headers, tags});
    check(response, {'AlterBrokerConfigs: Status is 204': response => response.status === 204});
    alterBrokerConfigsRequestLatency.add(response.timings.duration);
    alterBrokerConfigsRequestCount.add(1);
    return response;
}

let getBrokerConfigRequestLatency = new Trend('GetBrokerConfigRequestLatency', true);
let getBrokerConfigRequestCount = new Counter('GetBrokerConfigRequestCount');
export let getBrokerConfig = function (clusterId, brokerId, configName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}/configs/${configName}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetBrokerConfig'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetBrokerConfig: Status is 200': response => response.status === 200});
    getBrokerConfigRequestLatency.add(response.timings.duration);
    getBrokerConfigRequestCount.add(1);
    return response;
}

let updateBrokerConfigRequestLatency = new Trend('UpdateBrokerConfigRequestLatency', true);
let updateBrokerConfigRequestCount = new Counter('UpdateBrokerConfigRequestCount');
export let updateBrokerConfig = function (clusterId, brokerId, configName, newValue) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}/configs/${configName}`;
    const body = {value: newValue};
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'UpdateBrokerConfig'};
    let response = http.put(url, JSON.stringify(body), {headers, tags});
    check(response, {'UpdateBrokerConfig: Status is 204': response => response.status === 204});
    updateBrokerConfigRequestLatency.add(response.timings.duration);
    updateBrokerConfigRequestCount.add(1);
    return response;
}

let resetBrokerConfigRequestLatency = new Trend('ResetBrokerConfigRequestLatency', true);
let resetBrokerConfigRequestCount = new Counter('ResetBrokerConfigRequestCount');
export let resetBrokerConfig = function (clusterId, brokerId, configName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}/configs/${configName}`;
    const headers = {};
    const tags = {name: 'ResetBrokerConfig'};
    let response = http.del(url, {headers, tags});
    check(response, {'ResetBrokerConfig: Status is 204': response => response.status === 204});
    resetBrokerConfigRequestLatency.add(response.timings.duration);
    resetBrokerConfigRequestCount.add(1);
    return response;
}

let searchReplicasByBrokerRequestLatency = new Trend('SearchReplicasByBrokerRequestLatency', true);
let searchReplicasByBrokerRequestCount = new Counter('SearchReplicasByBrokerRequestCount');
export let searchReplicasByBroker = function (clusterId, brokerId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/brokers/${brokerId}/partition-replicas`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'SearchReplicasByBroker'};
    let response = http.get(url, {headers, tags});
    check(response, {'SearchReplicasByBroker: Status is 200': response => response.status === 200});
    searchReplicasByBrokerRequestLatency.add(response.timings.duration);
    searchReplicasByBrokerRequestCount.add(1);
    return response;
}

let listConsumerGroupsRequestLatency = new Trend('ListConsumerGroupsRequestLatency', true);
let listConsumerGroupsRequestCount = new Counter('ListConsumerGroupsRequestCount');
export let listConsumerGroups = function (clusterId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/consumer-groups`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListConsumerGroups'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListConsumerGroups: Status is 200': response => response.status === 200});
    listConsumerGroupsRequestLatency.add(response.timings.duration);
    listConsumerGroupsRequestCount.add(1);
    return response;
}

let getConsumerGroupRequestLatency = new Trend('GetConsumerGroupRequestLatency', true);
let getConsumerGroupRequestCount = new Counter('GetConsumerGroupRequestCount');
export let getConsumerGroup = function (clusterId, consumerGroupId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/consumer-groups/${consumerGroupId}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetConsumerGroup'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetConsumerGroup: Status is 200': response => response.status === 200});
    getConsumerGroupRequestLatency.add(response.timings.duration);
    getConsumerGroupRequestCount.add(1);
    return response;
}

let listConsumersRequestLatency = new Trend('ListConsumersRequestLatency', true);
let listConsumersRequestCount = new Counter('ListConsumersRequestCount');
export let listConsumers = function (clusterId, consumerGroupId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/consumer-groups/${consumerGroupId}/consumers`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListConsumers'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListConsumers: Status is 200': response => response.status === 200});
    listConsumersRequestLatency.add(response.timings.duration);
    listConsumersRequestCount.add(1);
    return response;
}

let getConsumerRequestLatency = new Trend('GetConsumerRequestLatency', true);
let getConsumerRequestCount = new Counter('GetConsumerRequestCount');
export let getConsumer = function (clusterId, consumerGroupId, consumerId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/consumer-groups/${consumerGroupId}/consumers/${consumerId}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetConsumer'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetConsumer: Status is 200': response => response.status === 200});
    getConsumerRequestLatency.add(response.timings.duration);
    getConsumerRequestCount.add(1);
    return response;
}

let listConsumerAssignmentsRequestLatency = new Trend('ListConsumerAssignmentsRequestLatency', true);
let listConsumerAssignmentsRequestCount = new Counter('ListConsumerAssignmentsRequestCount');
export let listConsumerAssignments = function (clusterId, consumerGroupId, consumerId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/consumer-groups/${consumerGroupId}/consumers/${consumerId}/assignments`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListConsumerAssignments'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListConsumerAssignments: Status is 200': response => response.status === 200});
    listConsumerAssignmentsRequestLatency.add(response.timings.duration);
    listConsumerAssignmentsRequestCount.add(1);
    return response;
}

let getConsumerAssignmentRequestLatency = new Trend('GetConsumerAssignmentRequestLatency', true);
let getConsumerAssignmentRequestCount = new Counter('GetConsumerAssignmentRequestCount');
export let getConsumerAssignment = function (clusterId, consumerGroupId, consumerId, topicName, partitionId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/consumer-groups/${consumerGroupId}/consumers/${consumerId}/assignments/${topicName}/partitions/${partitionId}`;
    const headers = {'Accept': 'application/json', 'Content-Type': 'application/json'};
    const tags = {name: 'GetConsumerAssignment'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetConsumerAssignment: Status is 200': response => response.status === 200});
    getConsumerAssignmentRequestLatency.add(response.timings.duration);
    getConsumerAssignmentRequestCount.add(1);
    return response;
}

let listTopicsRequestLatency = new Trend('ListTopicsRequestLatency', true);
let listTopicsRequestCount = new Counter('ListTopicsRequestCount');
export let listTopics = function (clusterId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListTopics'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListTopics: Status is 200': response => response.status === 200});
    listTopicsRequestLatency.add(response.timings.duration);
    listTopicsRequestCount.add(1);
    return response;
}

let createTopicRequestLatency = new Trend('CreateTopicRequestLatency', true);
let createTopicRequestCount = new Counter('CreateTopicRequestCount');
export let createTopic = function (clusterId, topicName, partitionsCount, replicationFactor) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics`;
    const body =
        {
            topic_name: topicName,
            partitions_count: partitionsCount,
            replication_factor: replicationFactor
        };
    const headers = {'Content-Type': 'application/json', 'Accept': 'application/json'};
    const tags = {name: 'CreateTopic'};
    let response = http.post(url, JSON.stringify(body), {headers, tags});
    check(response, {'CreateTopic: Status is 201': response => response.status === 201});
    createTopicRequestLatency.add(response.timings.duration);
    createTopicRequestCount.add(1);
    return response;
}

let getTopicRequestLatency = new Trend('GetTopicRequestLatency', true);
let getTopicRequestCount = new Counter('GetTopicRequestCount');
export let getTopic = function (clusterId, topicName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetTopic'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetTopic: Status is 200': response => response.status === 200});
    getTopicRequestLatency.add(response.timings.duration);
    getTopicRequestCount.add(1);
    return response;
}

let deleteTopicRequestLatency = new Trend('DeleteTopicRequestLatency', true);
let deleteTopicRequestCount = new Counter('DeleteTopicRequestCount');
export let deleteTopic = function (clusterId, topicName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}`;
    const headers = {}
    const tags = {name: 'DeleteTopic'};
    let response = http.del(url, {headers, tags});
    check(response, {'DeleteTopic: Status is 204': response => response.status === 204 || response.status === 404});
    deleteTopicRequestLatency.add(response.timings.duration);
    deleteTopicRequestCount.add(1);
    return response;
}

let listTopicConfigsRequestLatency = new Trend('ListTopicConfigsRequestLatency', true);
let listTopicConfigsRequestCount = new Counter('ListTopicConfigsRequestCount');
export let listTopicConfigs = function (clusterId, topicName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/configs`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListTopicConfigs'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListTopicConfigs: Status is 200': response => response.status === 200});
    listTopicConfigsRequestLatency.add(response.timings.duration);
    listTopicConfigsRequestCount.add(1);
    return response;
}

let alterTopicConfigsRequestLatency = new Trend('AlterTopicConfigsRequestLatency', true);
let alterTopicConfigsRequestCount = new Counter('AlterTopicConfigsRequestCount');
export let alterTopicConfigs = function (clusterId, topicName, configs) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/configs:alter`;
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'AlterTopicConfigs'};
    let response = http.post(url, JSON.stringify(configs), {headers, tags});
    check(response, {'AlterTopicConfigs: Status is 204': response => response.status === 204});
    alterTopicConfigsRequestLatency.add(response.timings.duration);
    alterTopicConfigsRequestCount.add(1);
    return response;
}

let getTopicConfigRequestLatency = new Trend('GetTopicConfigRequestLatency', true);
let getTopicConfigRequestCount = new Counter('GetTopicConfigRequestCount');
export let getTopicConfig = function (clusterId, topicName, configName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/configs/${configName}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetTopicConfig'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetTopicConfig: Status is 200': response => response.status === 200});
    getTopicConfigRequestLatency.add(response.timings.duration);
    getTopicConfigRequestCount.add(1);
    return response;
}

let updateTopicConfigRequestLatency = new Trend('UpdateTopicConfigRequestLatency', true);
let updateTopicConfigRequestCount = new Counter('UpdateTopicConfigRequestCount');
export let updateTopicConfig = function (clusterId, topicName, configName, newValue) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/configs/${configName}`;
    const body = {value: newValue};
    const headers = {'Content-Type': 'application/json'};
    const tags = {name: 'UpdateTopicConfig'};
    let response = http.put(url, JSON.stringify(body), {headers, tags});
    check(response, {'UpdateTopicConfig: Status is 204': response => response.status === 204});
    updateTopicConfigRequestLatency.add(response.timings.duration);
    updateTopicConfigRequestCount.add(1);
    return response;
}

let resetTopicConfigRequestLatency = new Trend('ResetTopicConfigRequestLatency', true);
let resetTopicConfigRequestCount = new Counter('ResetTopicConfigRequestCount');
export let resetTopicConfig = function (clusterId, topicName, configName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/configs/${configName}`;
    const headers = {};
    const tags = {name: 'ResetTopicConfig'};
    let response = http.del(url, {headers, tags});
    check(response, {'ResetTopicConfig: Status is 204': response => response.status === 204});
    resetTopicConfigRequestLatency.add(response.timings.duration);
    resetTopicConfigRequestCount.add(1);
    return response;
}

let listPartitionsRequestLatency = new Trend('ListPartitionsRequestLatency', true);
let listPartitionsRequestCount = new Counter('ListPartitionsRequestCount');
export let listPartitions = function (clusterId, topicName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/partitions`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListPartitions'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListPartitions: Status is 200': response => response.status === 200});
    listPartitionsRequestLatency.add(response.timings.duration);
    listPartitionsRequestCount.add(1);
    return response;
}

let getPartitionRequestLatency = new Trend('GetPartitionRequestLatency', true);
let getPartitionRequestCount = new Counter('GetPartitionRequestCount');
export let getPartition = function (clusterId, topicName, partitionId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionId}`;
    const headers = {'Accept': 'application/json', 'Content-Type': 'application/json'};
    const tags = {name: 'GetPartition'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetPartition: Status is 200': response => response.status === 200});
    getPartitionRequestLatency.add(response.timings.duration);
    getPartitionRequestCount.add(1);
    return response;
}

let listAllReplicaReassignmentsRequestLatency = new Trend('ListAllReplicaReassignmentsRequestLatency', true);
let listAllReplicaReassignmentsRequestCount = new Counter('ListAllReplicaReassignmentsRequestCount');
export let listAllReplicaReassignments = function (clusterId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/-/partitions/-/reassignment`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListAllReplicaReassignments'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListAllReplicaReassignments: Status is 200': response => response.status === 200});
    listAllReplicaReassignmentsRequestLatency.add(response.timings.duration);
    listAllReplicaReassignmentsRequestCount.add(1);
    return response;
}

let searchReplicaReassignmentByTopicRequestLatency = new Trend('SearchReplicaReassignmentByTopicRequestLatency', true);
let searchReplicaReassignmentByTopicRequestCount = new Counter('SearchReplicaReassignmentByTopicRequestCount');
export let searchReplicaReassignmentByTopic = function (clusterId, topicName) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/partitions/-/reassignment`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'SearchReplicaReassignmentByTopic'};
    let response = http.get(url, {headers, tags});
    check(response, {'SearchReplicaReassignmentByTopic: Status is 200': response => response.status === 200});
    searchReplicaReassignmentByTopicRequestLatency.add(response.timings.duration);
    searchReplicaReassignmentByTopicRequestCount.add(1);
    return response;
}

let getReplicaReassignmentsRequestLatency = new Trend('GetReplicaReassignmentsRequestLatency', true);
let getReplicaReassignmentsRequestCount = new Counter('GetReplicaReassignmentsRequestCount');
export let getReplicaReassignments = function (clusterId, topicName, partitionId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionId}/reassignment`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetReplicaReassignments'};
    let response = http.get(url, {headers, tags});
    // Expected to be 404. It is very hard (impossible?) to see a replica moving during test.
    check(response, {'GetReplicaReassignments: Status is 404': response => response.status === 404});
    getReplicaReassignmentsRequestLatency.add(response.timings.duration);
    getReplicaReassignmentsRequestCount.add(1);
    return response;
}

let listReplicasRequestLatency = new Trend('ListReplicasRequestLatency', true);
let listReplicasRequestCount = new Counter('ListReplicasRequestCount');
export let listReplicas = function (clusterId, topicName, partitionId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionId}/replicas`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'ListReplicas'};
    let response = http.get(url, {headers, tags});
    check(response, {'ListReplicas: Status is 200': response => response.status === 200});
    listReplicasRequestLatency.add(response.timings.duration);
    listReplicasRequestCount.add(1);
    return response;
}

let GetReplicaRequestLatency = new Trend('GetReplicaRequestLatency', true);
let GetReplicaRequestCount = new Counter('GetReplicaRequestCount');
export let getReplica = function (clusterId, topicName, partitionId, brokerId) {
    const url = `${baseUrl}/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionId}/replicas/${brokerId}`;
    const headers = {'Accept': 'application/json'};
    const tags = {name: 'GetReplica'};
    let response = http.get(url, {headers, tags});
    check(response, {'GetReplica: Status is 200': response => response.status === 200});
    GetReplicaRequestLatency.add(response.timings.duration);
    GetReplicaRequestCount.add(1);
    return response;
}
