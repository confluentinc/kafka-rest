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

echo "===> Configuring kafka-rest..."
env | grep ^KAFKA_REST \
  | sed -e 's/KAFKA_REST_\([^=]*\)/\L\1/' -e 's/=.*/\n&/' \
  | sed -e '/^[^=]/ s/_/./g' \
  | sed -e '/./{H;$!d} ; x ; s/\n=/=/g' \
  > /etc/kafka-rest/kafka-rest.properties

echo "===> Installed files:"
find /etc/kafka-rest/ /usr/share/kafka-rest/ -type f -print | sort

echo "===> Environment variables:"
env

echo "===> Launching kafka-rest..."
exec /usr/share/kafka-rest/bin/kafka-rest-start /etc/kafka-rest/kafka-rest.properties
