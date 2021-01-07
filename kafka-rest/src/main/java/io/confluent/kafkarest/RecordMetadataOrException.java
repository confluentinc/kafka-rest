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

package io.confluent.kafkarest;


import org.apache.kafka.clients.producer.RecordMetadata;

public class RecordMetadataOrException {

  private final RecordMetadata recordMetadata;
  private final Exception exception;

  public RecordMetadataOrException(RecordMetadata recordMetadata, Exception exception) {
    this.recordMetadata = recordMetadata;
    this.exception = exception;
  }

  public RecordMetadata getRecordMetadata() {
    return recordMetadata;
  }

  public Exception getException() {
    return exception;
  }
}
