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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;

@AutoValue
public abstract class CreateAclBatchRequest {

  CreateAclBatchRequest() {}

  @JsonValue
  public abstract CreateAclBatchRequestData getValue();

  public static CreateAclBatchRequest create(CreateAclBatchRequestData value) {
    return new AutoValue_CreateAclBatchRequest(value);
  }

  @JsonCreator
  static CreateAclBatchRequest fromJson(CreateAclBatchRequestData value) {
    return create(value);
  }
}
