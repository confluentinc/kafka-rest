/*
 * Copyright 2019 Confluent Inc.
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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nullable;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

/**
 * A {@link ParamConverterProvider} for {@link InstantConverter}.
 *
 * <p>By default it uses {@link DateTimeFormatter#ISO_INSTANT} format.</p>
 *
 * <p>TODO: If more formats are needed, create an annotation, e.g. DateTimeParam, and an
 * enum, e.g. DateTimeFormat, and map from enum value to DateTimeFormatter. The annotation will be
 * passed in {@link #getConverter(Class, Type, Annotation[])}.</p>
 */
@Provider
public final class InstantConverterProvider implements ParamConverterProvider {

  @Override
  @SuppressWarnings("unchecked")
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (!rawType.equals(Instant.class)) {
      return null;
    }
    return (ParamConverter<T>) new InstantConverter(DateTimeFormatter.ISO_INSTANT);
  }

  /**
   * A {@link ParamConverter} for {@link Instant}.
   */
  private static final class InstantConverter implements ParamConverter<Instant> {

    private final DateTimeFormatter formatter;

    private InstantConverter(DateTimeFormatter formatter) {
      this.formatter = requireNonNull(formatter, "formatter");
    }

    @Override
    @Nullable
    public Instant fromString(@Nullable String value) {
      // Contrary to JAX-RS specification, if the parameter is not present in the requested URI
      // Jersey will try to convert `null' instead of bypassing conversion and injecting `null'
      // directly. Return `null' here so that it can be passed to the injection point as expected.
      if (value == null) {
        return null;
      }
      return formatter.parse(value, Instant::from);
    }

    @Override
    public String toString(Instant value) {
      return formatter.format(value);
    }
  }
}
