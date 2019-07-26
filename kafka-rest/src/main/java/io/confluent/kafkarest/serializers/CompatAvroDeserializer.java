/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.serializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatAvroDeserializer extends KafkaAvroDeserializer {
  private static final Logger log = LoggerFactory.getLogger(CompatAvroDeserializer.class);

  @Override
  public Object deserialize(String s, byte[] bytes) {
    try {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.get();
      return super.deserialize(s, bytes);
    } catch (Exception e) {
      log.error("Failed to deserialize {}" + s, e.toString());
      return e.toString();
    }

  }

}
