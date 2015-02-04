/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.entities;

public interface TopicProduceRecord<K, V> extends ProduceRecord<K, V> {
  // It may seem odd that this is an interface when ProduceRecord<K,V> is an abstract class. If
  // we used an abstract class here and included the (Integer partition) field and
  // getters/setters, then subclasses would have to inherit from this class, and reuse of the
  // implementations of ProduceRecord would have to be via composition. This should be fine, but
  // currently it seems to be impossible to get Jackson to behave properly during
  // deserialization when combining that complex type hierarchy with it's unwrapping feature,
  // which would be required to get the serialize (key,value,partition) values at the same level.
  // This means implementations have a bit of duplication to provide the partition part of the
  // interface.

  public K getKey();

  public V getValue();

  public Integer getPartition();
}
