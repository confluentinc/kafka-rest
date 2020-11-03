package io.confluent.kafkarest.extension;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.annotation.JsonValue;
import java.lang.annotation.Annotation;
import javax.ws.rs.ext.ParamConverter;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
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

  @Test(expected = RuntimeException.class)
  public void enumWithMultipleJsonValueThrowsException() {
    converterProvider.getConverter(
        EnumWithMultipleJsonValue.class, EnumWithMultipleJsonValue.class, new Annotation[0]);
  }

  // TODO(rigelbm): Figure out why we are able to call a private method in an enum class.
  @Ignore
  @Test(expected = RuntimeException.class)
  public void enumWithPrivateJsonValueThrowsException() {
    converterProvider.getConverter(
        EnumWithPrivateJsonValue.class, EnumWithPrivateJsonValue.class, new Annotation[0]);
  }

  @Test(expected = RuntimeException.class)
  public void enumWithNonStringJsonValueThrowsException() {
    converterProvider.getConverter(
        EnumWithNonStringJsonValue.class, EnumWithNonStringJsonValue.class, new Annotation[0]);
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
