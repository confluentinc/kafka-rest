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

secrets_dir=$(dirname "$0")

function gencacert() {
    local ca_name="$1"

    openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \
      -keyout "${secrets_dir}/${ca_name}.key" \
      -out "${secrets_dir}/${ca_name}.crt" \
      -subj "/CN=${ca_name}" \
      -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

    keytool -keystore "${secrets_dir}/${ca_name}.jks" \
      -alias "$ca_name" \
      -importcert -file "${secrets_dir}/${ca_name}.crt" \
      -storepass "$ca_name"-pass \
      -noprompt
}

function genleafcert() {
  local name="$1"
  local ca_name="$2"

  openssl req -new -newkey rsa:4096 -nodes \
    -keyout "${secrets_dir}/${name}.key" \
    -out "${secrets_dir}/${name}.csr" \
    -subj "/CN=${name}" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

  openssl x509 -req -days 365 \
    -CA "${secrets_dir}/${ca_name}.crt" -CAkey "${secrets_dir}/${ca_name}.key" \
    $([[ -f "${secrets_dir}/${ca_name}.srl" ]] \
      && echo "-CAserial ${secrets_dir}/${ca_name}.srl" \
      || echo "-CAcreateserial") \
    -extensions SAN \
    -extfile <(printf "[SAN]\nsubjectAltName=DNS:localhost,IP:127.0.0.1") \
    -in "${secrets_dir}/${name}.csr" \
    -out "${secrets_dir}/${name}.crt"

  openssl pkcs12 -export \
    -in "${secrets_dir}/${name}.crt" -inkey "${secrets_dir}/${name}.key" \
    -name "$name" \
    -password pass:"$name"-pass \
    > "${secrets_dir}/${name}.p12"

  keytool -importkeystore \
    -srckeystore "${secrets_dir}/${name}.p12" \
    -srcstoretype pkcs12 \
    -srcstorepass "$name"-pass \
    -destkeystore "${secrets_dir}/${name}.jks" \
    -deststorepass "$name"-pass \
    -destkeypass "$name"-pass \
    -alias "$name"
}

rm -f "$secrets_dir"/*.crt "$secrets_dir"/*.csr "$secrets_dir"/*.jks "$secrets_dir"/*.key "$secrets_dir"/*.srl

gencacert kafka-ca

genleafcert kafka-1 kafka-ca
genleafcert kafka-2 kafka-ca
genleafcert kafka-3 kafka-ca
genleafcert kafka-rest kafka-ca
genleafcert alice kafka-ca
genleafcert bob kafka-ca
