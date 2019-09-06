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

public class ConsumerInstanceId {

  private final String group;
  private final String instance;

  public ConsumerInstanceId(String group, String instance) {
    this.group = group;
    this.instance = instance;
  }

  public String getGroup() {
    return group;
  }

  public String getInstance() {
    return instance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerInstanceId that = (ConsumerInstanceId) o;

    if (group != null ? !group.equals(that.group) : that.group != null) {
      return false;
    }
    if (instance != null ? !instance.equals(that.instance) : that.instance != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = group != null ? group.hashCode() : 0;
    result = 31 * result + (instance != null ? instance.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ConsumerInstanceId{"
           + "group='" + group + '\''
           + ", instance='" + instance + '\''
           + '}';
  }
}

