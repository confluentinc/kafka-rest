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

# Use -d to start containers in detached mode.
detach=

while getopts "d" opt
do
  case "$opt" in
    d) detach=-d;;
    *) echo "Usage: $0 [-d]" >&2
       exit 1 ;;
  esac
done
shift "$((OPTIND - 1))"

env_dir=$(dirname "$0")
base_dir=$env_dir/../../..

# Login to nightly docker repository.
aws ecr get-login-password --region us-west-2 \
 | docker login --username AWS --password-stdin 368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/

# Download latest cp-zookeeper image.
docker pull 368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-zookeeper:6.0.x-latest

# Download latest cp-server image.
docker pull 368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-server:6.0.x-latest

# Download latest cp-kafka-rest image.
docker pull 368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-kafka-rest:6.0.x-latest

# Make sure kafka-rest is packaged.
mvn -f "$base_dir"/kafka-rest/pom.xml -Dmaven.test.skip=true clean package

# For some reason `up --build --force-recreate` is not enough. Make sure everything is clean.
docker-compose -f "$env_dir"/docker-compose.yml rm -fsv

# Start all containers.
docker-compose -f "$env_dir"/docker-compose.yml up --build --force-recreate $detach
