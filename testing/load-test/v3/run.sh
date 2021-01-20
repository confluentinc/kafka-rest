#!/bin/bash
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

function wait_for_server() {
  local -r server=$1

  until [[ "$(curl -s -o /dev/null -w '%{http_code}' "${server}")" == "200" ]]; do
    sleep 1
  done
}

function wait_for_server_with_timeout() {
  local -r server=$1
  local -r timeout=$2

  declare -fxr wait_for_server
  timeout "${timeout}" bash -c "wait_for_server ${server}"
}

test_dir=$(dirname "$0")
base_dir="${test_dir}"/../../..
envs_dir="${base_dir}"/testing/environments
target_dir="${test_dir}"/target

if [ -d "${target_dir}" ]; then rm -Rf "${target_dir}"; fi
mkdir "${target_dir}"

for test_file in "${test_dir}"/*-test.js; do
  test_name="${test_file%.js}"
  test_name="${test_name##*/}"
  "${envs_dir}"/sasl_plain/run.sh -d

  wait_for_server_with_timeout http://localhost:9391 180s

  k6 run "${test_dir}/${test_name}.js" --summary-export="${test_dir}/target/${test_name}.out"
done

python "${base_dir}"/testing/load-test/collect-results.py \
  "${target_dir}"/*.out \
  "${target_dir}/summary.xlsx"
