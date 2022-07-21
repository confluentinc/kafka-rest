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

import static java.util.Objects.requireNonNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;

public final class EnumConverterProvider implements ParamConverterProvider {

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (!Enum.class.isAssignableFrom(rawType)) {
      return null;
    }
    return (ParamConverter<T>) new EnumConverter<>((Class<Enum>) rawType);
  }

  private static final class EnumConverter<T extends Enum<T>>
      implements ParamConverter<T> {

    private final Class<T> enumClass;

    private EnumConverter(Class<T> enumClass) {
      this.enumClass = requireNonNull(enumClass);
    }

    @Override
    public T fromString(String value) {
      if (value == null) {
        return null;
      }
      return Enum.valueOf(enumClass, value.toUpperCase());
    }

    @Override
    public String toString(T value) {
      throw new UnsupportedOperationException();
    }
  }
}
