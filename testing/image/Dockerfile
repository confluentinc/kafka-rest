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

FROM 368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-kafka-rest:6.0.x-latest

COPY bin /usr/bin/

COPY kafka-rest/target/kafka-rest-*-package/share/java/kafka-rest-bin /usr/share/java/kafka-rest-bin/
COPY kafka-rest/target/kafka-rest-*-package/share/java/kafka-rest-lib /usr/share/java/kafka-rest-lib/
