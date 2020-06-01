/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.controllers;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import javax.ws.rs.NotFoundException;

final class Entities {

  private Entities() {
  }

  static <T> T checkEntityExists(Optional<T> entity, String message, Object... args) {
    return entity.orElseThrow(() -> new NotFoundException(String.format(message, args)));
  }

  static <T, K> Optional<T> findEntityByKey(Collection<T> entities, Function<T, K> toKey, K key) {
    return entities.stream()
        .filter(
            entity -> {
              K entityKey = toKey.apply(entity);
              return (entityKey == null && key == null)
                  || (entityKey != null && entityKey.equals(key));
            })
        .findAny();
  }
}
