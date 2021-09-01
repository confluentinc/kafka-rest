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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;

public final class EnumConverterProvider implements ParamConverterProvider {

  private static final Table<Class<?>, String, Enum<?>> enumMapper =
      Tables.synchronizedTable(HashBasedTable.create());

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (!Enum.class.isAssignableFrom(rawType)) {
      return null;
    }

    ParamConverter<T> converter = (ParamConverter<T>) new EnumConverter<>((Class<Enum>) rawType);
    if (enumMapper.containsRow(rawType)) {
      return converter;
    }

    List<Method> jsonValueMethods =
        Arrays.stream(rawType.getMethods())
            .filter(method -> method.getAnnotation(JsonValue.class) != null)
            .collect(Collectors.toList());

    if (jsonValueMethods.size() > 1) {
      throw new RuntimeException(
          "Multiple methods annotated with @JsonValue in " + rawType.getName());
    }

    if (jsonValueMethods.isEmpty()) {
      for (T constant : rawType.getEnumConstants()) {
        Enum<?> casted = (Enum<?>) constant;
        enumMapper.put(rawType, casted.name().toUpperCase(), casted);
      }
      return converter;
    }

    Method jsonValueMethod = jsonValueMethods.get(0);
    for (T constant : rawType.getEnumConstants()) {
      String returnedValue;
      try {
        returnedValue = (String) jsonValueMethod.invoke(constant);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(
            String.format("@JsonValue annotated method in %s is not public.", rawType.getName()));
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (ClassCastException e) {
        throw new RuntimeException(
            String.format(
                "@JsonValue annotated method in %s does not return String.", rawType.getName()));
      }

      enumMapper.put(rawType, returnedValue.toUpperCase(), (Enum<?>) constant);
    }

    return converter;
  }

  private static final class EnumConverter<T extends Enum<T>> implements ParamConverter<T> {

    private final Class<T> enumClass;

    private EnumConverter(Class<T> enumClass) {
      this.enumClass = requireNonNull(enumClass);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromString(String value) {
      if (value == null) {
        return null;
      }

      T cached = (T) enumMapper.get(enumClass, value.toUpperCase());
      if (cached == null) {
        throw new RuntimeException(
            String.format("No constant in %s matched %s", enumClass.getName(), value));
      }
      return cached;
    }

    @Override
    public String toString(T value) {
      throw new UnsupportedOperationException();
    }
  }
}
