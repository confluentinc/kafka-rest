package io.confluent.kafkarest.response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
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
