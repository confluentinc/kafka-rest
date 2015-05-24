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

public interface ProduceRecord<K, V> {

  public K getKey();

  public V getValue();

  // Non-standard naming so we can unify the interfaces of ProduceRecord and TopicProduceRecord,
  // but get Jackson to behave properly, not serializing the value & triggering errors if the
  // field is present during deserialization for types where it should always be null.
  public Integer partition();
}