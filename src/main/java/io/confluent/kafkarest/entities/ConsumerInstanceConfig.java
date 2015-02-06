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

import javax.validation.constraints.NotNull;

public class ConsumerInstanceConfig {

  private static final EmbeddedFormat DEFAULT_FORMAT = EmbeddedFormat.BINARY;

  private String id;
  @NotNull
  private EmbeddedFormat format;
  private String autoOffsetReset;
  private String autoCommitEnable;

  public ConsumerInstanceConfig() {
    this(DEFAULT_FORMAT);
  }

  public ConsumerInstanceConfig(EmbeddedFormat format) {
    this(null, format, null, null);
  }

  public ConsumerInstanceConfig(@JsonProperty("id") String id,
                                @JsonProperty("format") EmbeddedFormat format,
                                @JsonProperty("auto.offset.reset") String autoOffsetReset,
                                @JsonProperty("auto.commit.enable") String autoCommitEnable) {
    this.id = id;
    this.format = format != null ? format : DEFAULT_FORMAT;
    this.autoOffsetReset = autoOffsetReset;
    this.autoCommitEnable = autoCommitEnable;
  }

  @JsonProperty
  public String getId() {
    return id;
  }

  @JsonProperty
  public void setId(String id) {
    this.id = id;
  }

  @JsonProperty
  public EmbeddedFormat getFormat() {
    return format;
  }

  @JsonProperty
  public void setFormat(EmbeddedFormat format) {
    this.format = format;
  }

  @JsonProperty("auto.offset.reset")
  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  @JsonProperty("auto.offset.reset")
  public void setAutoOffsetReset(String autoOffsetReset) {
    this.autoOffsetReset = autoOffsetReset;
  }

  @JsonProperty("auto.commit.enable")
  public String getAutoCommitEnable() {
    return autoCommitEnable;
  }

  @JsonProperty("auto.commit.enable")
  public void setAutoCommitEnable(String autoCommitEnable) {
    this.autoCommitEnable = autoCommitEnable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerInstanceConfig that = (ConsumerInstanceConfig) o;

    if (autoCommitEnable != null ? !autoCommitEnable.equals(that.autoCommitEnable)
                                 : that.autoCommitEnable != null) {
      return false;
    }
    if (autoOffsetReset != null ? !autoOffsetReset.equals(that.autoOffsetReset)
                                : that.autoOffsetReset != null) {
      return false;
    }
    if (format != that.format) {
      return false;
    }
    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (format != null ? format.hashCode() : 0);
    result = 31 * result + (autoOffsetReset != null ? autoOffsetReset.hashCode() : 0);
    result = 31 * result + (autoCommitEnable != null ? autoCommitEnable.hashCode() : 0);
    return result;
  }
}
