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

public class SchemaHolder {

  protected String keySchema;
  protected Integer keySchemaId;

  protected String valueSchema;
  protected Integer valueSchemaId;

  public SchemaHolder() {
  }

  public SchemaHolder(String keySchema, String valueSchema) {
    this(keySchema, null, valueSchema, null);
  }

  public SchemaHolder(String keySchema, Integer keySchemaId,
                      String valueSchema, Integer valueSchemaId) {
    this.keySchema = keySchema;
    this.keySchemaId = keySchemaId;
    this.valueSchema = valueSchema;
    this.valueSchemaId = valueSchemaId;
  }

  @JsonProperty("key_schema")
  public String getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(String keySchema) {
    this.keySchema = keySchema;
  }

  @JsonProperty("key_schema_id")
  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  public void setKeySchemaId(Integer keySchemaId) {
    this.keySchemaId = keySchemaId;
  }

  @JsonProperty("value_schema")
  public String getValueSchema() {
    return valueSchema;
  }

  public void setValueSchema(String valueSchema) {
    this.valueSchema = valueSchema;
  }

  @JsonProperty("value_schema_id")
  public Integer getValueSchemaId() {
    return valueSchemaId;
  }

  public void setValueSchemaId(Integer valueSchemaId) {
    this.valueSchemaId = valueSchemaId;
  }
}
