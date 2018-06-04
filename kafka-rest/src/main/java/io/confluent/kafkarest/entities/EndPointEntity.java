/*
 * Copyright 2017 Confluent Inc.
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
 */

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains broker endpoint information
 */
public class EndPointEntity {

  private String protocol;
  private String host;
  private Integer port;

  @JsonCreator
  public EndPointEntity(@JsonProperty("protocol") String protocol,
                        @JsonProperty("host") String host,
                        @JsonProperty("port") Integer port) {
    this.protocol = protocol;
    this.host = host;
    this.port = port;
  }

  @JsonProperty
  public String getProtocol() {
    return protocol;
  }

  @JsonProperty
  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  @JsonProperty
  public String getHost() {
    return host;
  }

  @JsonProperty
  public void setHost(String host) {
    this.host = host;
  }

  @JsonProperty
  public Integer getPort() {
    return port;
  }

  @JsonProperty
  public void setPort(Integer port) {
    this.port = port;
  }

  @Override
  public String toString() {
    return "EndPointEntity{"
            + "protocol='" + protocol + '\''
            + ", host='" + host + '\''
            + ", port=" + port
            + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EndPointEntity that = (EndPointEntity) o;

    if (!protocol.equals(that.protocol)) {
      return false;
    }
    if (!host.equals(that.host)) {
      return false;
    }
    return port.equals(that.port);
  }

  @Override
  public int hashCode() {
    int result = protocol.hashCode();
    result = 31 * result + host.hashCode();
    result = 31 * result + port.hashCode();
    return result;
  }
}
