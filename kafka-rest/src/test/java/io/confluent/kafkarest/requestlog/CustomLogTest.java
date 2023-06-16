/*
 * Copyright 2023 Confluent Inc.
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
 * specific language governing permissions and limitations under the License
 */

package io.confluent.kafkarest.requestlog;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.easymock.EasyMock;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CustomLogTest {

  private String logEntry;

  private class TestLogWriter implements RequestLog.Writer {

    @Override
    public void write(final String s) throws IOException {
      logEntry = s;
    }
  }

  @BeforeEach
  public void setUp() {
    logEntry = "";
  }

  @Test
  public void test_IfNoAttributeConfigured_ThenLogMatchesFormat() throws Exception {
    CustomLog customLog =
        new CustomLog(new TestLogWriter(), "%{client}a %H %{User-Agent}i", new String[] {});
    customLog.start();

    Request request = mock(Request.class);
    expect(request.getRemoteHost()).andReturn("localhost");
    expect(request.getProtocol()).andReturn("testProtocol");
    expect(request.getHeader("User-Agent")).andReturn("testUser");

    Response response = mock(Response.class);
    replay(request, response);

    customLog.log(request, response);
    verify(request, response);

    assertEquals("localhost testProtocol testUser", logEntry);
  }

  @Test
  public void test_IfAttributeConfiguredAndSetOnRequest_And_ThenLogged() throws Exception {
    CustomLog customLog =
        new CustomLog(
            new TestLogWriter(), "%{client}a %H %{User-Agent}i", new String[] {"FooBarAttr"});
    customLog.start();

    Request request = mock(Request.class);
    Response response = mock(Response.class);

    // Before Jetty's CustomRequestLog.log() is called, need to convert request-attribute ->
    // response-header for logging.
    expect(request.getAttribute("FooBarAttr")).andReturn("FooBarVal");
    request.removeAttribute("FooBarAttr");
    EasyMock.expectLastCall();
    response.setHeader("FooBarAttr", "FooBarVal");
    EasyMock.expectLastCall();

    // Needed by CustomRequestLog.log()
    expect(request.getRemoteHost()).andReturn("localhost");
    expect(request.getProtocol()).andReturn("testProtocol");
    expect(request.getHeader("User-Agent")).andReturn("testUser");
    expect(response.getHeader("FooBarAttr")).andReturn("FooBarVal");

    // After CustomRequestLog.log() make sure response-headers, added only for logging, are removed.
    HttpFields httpFields = mock(HttpFields.class);
    expect(response.getHttpFields()).andReturn(httpFields);
    expect(httpFields.remove("FooBarAttr")).andReturn(null);
    replay(request, response);

    customLog.log(request, response);
    verify(request, response);

    assertEquals("localhost testProtocol testUser FooBarVal", logEntry);
  }

  @Test
  public void test_IfAttributeConfiguredAndNotSetOnRequest_And_ThenNotLogged() throws Exception {
    CustomLog customLog =
        new CustomLog(
            new TestLogWriter(), "%{client}a %H %{User-Agent}i", new String[] {"FooBarAttr"});
    customLog.start();

    Request request = mock(Request.class);
    Response response = mock(Response.class);

    // Before Jetty's CustomRequestLog.log() is called, configured request-attribute will be checked
    // but don't expect any response-headers to be set if attribute isn't set(i.e. null)
    expect(request.getAttribute("FooBarAttr")).andReturn(null);

    // Needed by CustomRequestLog.log()
    expect(request.getRemoteHost()).andReturn("localhost");
    expect(request.getProtocol()).andReturn("testProtocol");
    expect(request.getHeader("User-Agent")).andReturn("testUser");
    expect(response.getHeader("FooBarAttr")).andReturn(null);

    // After CustomRequestLog.log() make sure response-headers, added only for logging, are removed.
    HttpFields httpFields = mock(HttpFields.class);
    expect(response.getHttpFields()).andReturn(httpFields);
    expect(httpFields.remove("FooBarAttr")).andReturn(null);
    replay(request, response);

    customLog.log(request, response);
    verify(request, response);

    assertEquals("localhost testProtocol testUser -", logEntry);
  }
}
