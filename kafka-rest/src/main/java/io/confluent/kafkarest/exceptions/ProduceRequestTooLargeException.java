/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafkarest.exceptions;

/** This exception is thrown when a produce request exceeds the size threshold. */
public class ProduceRequestTooLargeException extends RuntimeException {
  public ProduceRequestTooLargeException() {
    this("Produce request size is larger than allowed threshold");
  }

  public ProduceRequestTooLargeException(String message) {
    super(message);
  }
}
