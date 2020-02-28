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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FakeAsyncResponseTest {

  @Test
  public void resumeWithValueThenGetValue_returnsValue() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume("foobar");
    assertEquals("foobar", response.getValue());
  }

  @Test
  public void resumeWithValueThenGetException_returnsNull() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume("foobar");
    assertNull(response.getException());
  }

  @Test
  public void resumeWithExceptionThenGetValue_returnsNull() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume(new IllegalArgumentException());
    assertNull(response.getValue());
  }

  @Test
  public void resumeWithExceptionThenGetException_returnsException() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume(new IllegalArgumentException());
    assertEquals(IllegalArgumentException.class, response.getException().getClass());
  }

  @Test
  public void getValue_supended_throwsIllegalStateException() {
    FakeAsyncResponse response = new FakeAsyncResponse();

    try {
      response.getValue();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void getException_supended_throwsIllegalStateException() {
    FakeAsyncResponse response = new FakeAsyncResponse();

    try {
      response.getException();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void resumeWithValueThenIsDone_returnsTrue() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume("foobar");
    assertTrue(response.isDone());
    assertFalse(response.isSuspended());
    assertFalse(response.isCancelled());
  }

  @Test
  public void resumeWithExceptionThenIsDone_returnsTrue() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume(new IllegalArgumentException());
    assertTrue(response.isDone());
    assertFalse(response.isSuspended());
    assertFalse(response.isCancelled());
  }

  @Test
  public void noResumeOrCancellationThenIsSuspended_returnsTrue() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    assertTrue(response.isSuspended());
    assertFalse(response.isDone());
    assertFalse(response.isCancelled());
  }

  @Test
  public void resume_notSuspended_returnsFalse() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume("foobar");
    assertFalse(response.resume("foobar"));
    assertFalse(response.resume(new IllegalArgumentException()));
  }

  @Test
  public void cancel_notSuspended_returnsFalse() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.resume("foobar");
    assertFalse(response.cancel());
  }

  @Test
  public void cancelThenIsCancelled_returnsTrue() {
    FakeAsyncResponse response = new FakeAsyncResponse();
    response.cancel();
    assertTrue(response.isCancelled());
    assertFalse(response.isSuspended());
    assertFalse(response.isDone());
  }
}
