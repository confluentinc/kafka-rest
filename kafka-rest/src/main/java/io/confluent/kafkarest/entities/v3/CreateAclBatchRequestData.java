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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;

// import io.confluent.kafkarest.entities.Acl;

@AutoValue
public abstract class CreateAclBatchRequestData {

  CreateAclBatchRequestData() {}

  @JsonProperty("data")
  public abstract ImmutableList<CreateAclRequest> getData();

  public static CreateAclBatchRequestData create(List<CreateAclRequest> data) {
    return new AutoValue_CreateAclBatchRequestData(ImmutableList.copyOf(data));
  }

  @JsonCreator
  static CreateAclBatchRequestData fromJson(@JsonProperty("data") List<CreateAclRequest> data) {
    return create(data);
  }
}
