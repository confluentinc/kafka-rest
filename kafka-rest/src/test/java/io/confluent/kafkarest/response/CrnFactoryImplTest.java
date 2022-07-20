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
