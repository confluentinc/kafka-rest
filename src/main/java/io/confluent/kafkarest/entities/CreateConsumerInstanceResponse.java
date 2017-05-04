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

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

public class CreateConsumerInstanceResponse {

  @NotBlank
  String instanceId;

  @NotBlank
  @URL
  String baseUri;

  public CreateConsumerInstanceResponse(
      @JsonProperty("instance_id") String instanceId,
      @JsonProperty("base_uri") String baseUri
  ) {
    this.instanceId = instanceId;
    this.baseUri = baseUri;
  }

  @JsonProperty("instance_id")
  public String getInstanceId() {
    return instanceId;
  }

  @JsonProperty("instance_id")
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  @JsonProperty("base_uri")
  public String getBaseUri() {
    return baseUri;
  }

  @JsonProperty("base_uri")
  public void setBaseUri(String baseUri) {
    this.baseUri = baseUri;
  }
}
