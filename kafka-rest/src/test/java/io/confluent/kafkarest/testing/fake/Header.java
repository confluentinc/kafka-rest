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

import com.google.protobuf.ByteString;
import java.util.Optional;

final class Header {
  private final String key;
  private final Optional<ByteString> value;

  Header(String key, Optional<ByteString> value) {
    this.key = requireNonNull(key);
    this.value = requireNonNull(value);
  }

  String getKey() {
    return key;
  }

  Optional<ByteString> getValue() {
    return value;
  }
}
