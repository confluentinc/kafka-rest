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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonValue;
import java.lang.annotation.Annotation;
import javax.ws.rs.ext.ParamConverter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class EnumConverterProviderTest {

  private final EnumConverterProvider converterProvider = new EnumConverterProvider();

  @Test
  public void enumWithNoJsonValueMatchesConstantName() {
    ParamConverter<EnumWithNoJsonValue> converter =
        converterProvider.getConverter(
            EnumWithNoJsonValue.class, EnumWithNoJsonValue.class, new Annotation[0]);

    assertNotNull(converter);
    assertEquals(EnumWithNoJsonValue.FOO, converter.fromString("foo"));
    assertEquals(EnumWithNoJsonValue.BAR, converter.fromString("BAR"));
  }

  @Test
  public void enumWithSingleJsonValueMatchesOnJsonValue() {
    ParamConverter<EnumWithSingleJsonValue> converter =
        converterProvider.getConverter(
            EnumWithSingleJsonValue.class, EnumWithSingleJsonValue.class, new Annotation[0]);

    assertNotNull(converter);
    assertEquals(EnumWithSingleJsonValue.FOO, converter.fromString("abc"));
    assertEquals(EnumWithSingleJsonValue.BAR, converter.fromString("123"));
  }

  @Test
  public void enumWithMultipleJsonValueThrowsException() {
    assertThrows(
        RuntimeException.class,
        () ->
            converterProvider.getConverter(
                EnumWithMultipleJsonValue.class,
                EnumWithMultipleJsonValue.class,
                new Annotation[0]));
  }

  // TODO(rigelbm): Figure out why we are able to call a private method in an enum class.
  @Disabled
  @Test
  public void enumWithPrivateJsonValueThrowsException() {
    assertThrows(
        RuntimeException.class,
        () ->
            converterProvider.getConverter(
                EnumWithPrivateJsonValue.class, EnumWithPrivateJsonValue.class, new Annotation[0]));
  }

  @Test
  public void enumWithNonStringJsonValueThrowsException() {
    assertThrows(
        RuntimeException.class,
        () ->
            converterProvider.getConverter(
                EnumWithNonStringJsonValue.class,
                EnumWithNonStringJsonValue.class,
                new Annotation[0]));
  }

  public enum EnumWithNoJsonValue {
    FOO("abc"),

    BAR("123");

    private final String foobar;

    EnumWithNoJsonValue(String foobar) {
      this.foobar = foobar;
    }

    public String getFoobar() {
      return foobar;
    }
  }

  public enum EnumWithSingleJsonValue {
    FOO("abc"),

    BAR("123");

    private final String foobar;

    EnumWithSingleJsonValue(String foobar) {
      this.foobar = foobar;
    }

    @JsonValue
    public String getFoobar() {
      return foobar;
    }
  }

  public enum EnumWithMultipleJsonValue {
    FOO("abc"),

    BAR("123");

    private final String foobar;

    EnumWithMultipleJsonValue(String foobar) {
      this.foobar = foobar;
    }

    @JsonValue
    public String getFoobar() {
      return foobar;
    }

    @JsonValue
    public String getFozbaz() {
      return foobar;
    }
  }

  private enum EnumWithPrivateJsonValue {
    FOO("abc"),

    BAR("123");

    private final String foobar;

    EnumWithPrivateJsonValue(String foobar) {
      this.foobar = foobar;
    }

    @JsonValue
    private String getFoobar() {
      return foobar;
    }
  }

  public enum EnumWithNonStringJsonValue {
    FOO("abc"),

    BAR("123");

    private final String foobar;

    EnumWithNonStringJsonValue(String foobar) {
      this.foobar = foobar;
    }

    @JsonValue
    public int getFoobar() {
      return foobar.length();
    }
  }
}
