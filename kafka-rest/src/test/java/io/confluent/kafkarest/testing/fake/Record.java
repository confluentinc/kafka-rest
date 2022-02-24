/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.testing.fake;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

final class Record {
  private final ImmutableList<Header> headers;
  private final Optional<ByteString> key;
  private final Optional<ByteString> value;
  private final Instant timestamp;

  Record(
      List<Header> headers,
      Optional<ByteString> key,
      Optional<ByteString> value,
      Instant timestamp) {
    this.headers = ImmutableList.copyOf(headers);
    this.key = requireNonNull(key);
    this.value = requireNonNull(value);
    this.timestamp = requireNonNull(timestamp);
  }

  ImmutableList<Header> getHeaders() {
    return headers;
  }

  Optional<ByteString> getKey() {
    return key;
  }

  Optional<ByteString> getValue() {
    return value;
  }

  Instant getTimestamp() {
    return timestamp;
  }
}
