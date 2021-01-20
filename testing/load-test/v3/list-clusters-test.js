import {listClusters}  from './common.js'

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
};

export function setup() {
    return {};
}

export default function (data) {
    listClusters();
}

export function teardown(data) {
}
