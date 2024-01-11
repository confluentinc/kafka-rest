package io.confluent.kafkarest.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

public class CrnFactoryImplTest {

  @Test
  public void create_noAuthority_returnsCrnWithNoAuthority() {
    CrnFactoryImpl crnFactory = new CrnFactoryImpl(/* crnAuthorityConfig= */ "");
    assertEquals("crn:///foo=xxx/bar=yyy", crnFactory.create("foo", "xxx", "bar", "yyy"));
  }

  @Test
  public void create_withAuthority_returnsCrnWithAuthority() {
    CrnFactoryImpl crnFactory = new CrnFactoryImpl(/* crnAuthorityConfig= */ "example.com");
    assertEquals(
        "crn://example.com/foo=xxx/bar=yyy", crnFactory.create("foo", "xxx", "bar", "yyy"));
  }

  @Test
  public void create_oddComponents_throwsIllegalArgument() {
    CrnFactoryImpl crnFactory = new CrnFactoryImpl(/* crnAuthorityConfig= */ "");
    try {
      crnFactory.create("foo", "xxx", "bar");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
