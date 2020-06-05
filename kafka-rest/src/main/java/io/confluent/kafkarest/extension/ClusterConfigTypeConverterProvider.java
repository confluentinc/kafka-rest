/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.extension;

import io.confluent.kafkarest.entities.ClusterConfig;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;

public final class ClusterConfigTypeConverterProvider implements ParamConverterProvider {

  @Override
  @SuppressWarnings("unchecked")
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (!rawType.equals(ClusterConfig.Type.class)) {
      return null;
    }
    return (ParamConverter<T>) ClusterConfigTypeConverter.INSTANCE;
  }

  private enum ClusterConfigTypeConverter implements ParamConverter<ClusterConfig.Type> {
    INSTANCE;

    @Override
    public ClusterConfig.Type fromString(String value) {
      if (value == null) {
        return null;
      }
      return ClusterConfig.Type.valueOf(value.toUpperCase());
    }

    @Override
    public String toString(ClusterConfig.Type value) {
      return value.toString().toLowerCase();
    }
  }
}
