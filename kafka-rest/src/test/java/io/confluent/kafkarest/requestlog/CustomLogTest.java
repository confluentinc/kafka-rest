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
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("KNET-18040")
public class CustomLogTest {

  private String logEntry;

  private static String LOG_FORMAT = "%m %uri";

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
    CustomLog customLog = new CustomLog(new TestLogWriter(), LOG_FORMAT, new String[] {});
    customLog.start();

    Request request = mock(Request.class);
    expect(request.getMethod()).andReturn("PATCH");
    expect(request.getHttpURI()).andReturn(HttpURI.from("testUri"));

    Response response = mock(Response.class);
    replay(request, response);

    customLog.log(request, response);
    verify(request, response);

    assertEquals("PATCH testUri", logEntry);
  }

  @Test
  public void test_IfAttributeConfiguredAndSetOnRequest_And_ThenLogged() throws Exception {
    CustomLog customLog =
        new CustomLog(new TestLogWriter(), LOG_FORMAT, new String[] {"FooBarAttr"});
    customLog.start();

    Request request = mock(Request.class);
    Response response = mock(Response.class);
    HttpFields.Mutable httpFields = mock(HttpFields.Mutable.class);

    // Before Jetty's CustomRequestLog.log() is called, need to convert request-attribute ->
    // response-header for logging.
    expect(request.getAttribute("FooBarAttr")).andReturn("FooBarVal");
    expect(request.removeAttribute("FooBarAttr")).andReturn(null);
    // Set response-header for logging
    expect(response.getHeaders()).andReturn(httpFields).anyTimes();
    expect(httpFields.put("FooBarAttr", "FooBarVal")).andReturn(httpFields);

    // Setup request values for logging
    expect(request.getMethod()).andReturn("PATCH");
    expect(request.getHttpURI()).andReturn(HttpURI.from("testUri"));
    expect(httpFields.get("FooBarAttr")).andReturn("FooBarVal");

    // After CustomRequestLog.log() make sure response-headers added only for logging, are removed.
    expect(httpFields.remove("FooBarAttr")).andReturn(httpFields);

    replay(request, response, httpFields);

    customLog.log(request, response);
    verify(request, response, httpFields);

    assertEquals("PATCH testUri FooBarVal", logEntry);
  }

  @Test
  public void test_IfAttributeConfiguredAndNotSetOnRequest_And_ThenNotLogged() throws Exception {
    CustomLog customLog =
        new CustomLog(new TestLogWriter(), LOG_FORMAT, new String[] {"FooBarAttr"});
    customLog.start();

    Request request = mock(Request.class);
    Response response = mock(Response.class);
    HttpFields.Mutable httpFields = mock(HttpFields.Mutable.class);

    // Before Jetty's CustomRequestLog.log() is called, configured request-attribute will be checked
    // but don't expect any response-headers to be set if attribute isn't set(i.e. null)
    expect(request.getAttribute("FooBarAttr")).andReturn(null);

    // Needed by CustomRequestLog.log()
    expect(request.getMethod()).andReturn("PATCH");
    expect(request.getHttpURI()).andReturn(HttpURI.from("testUri"));
    expect(httpFields.get("FooBarAttr")).andReturn(null);

    // After CustomRequestLog.log() make sure response-headers added only for logging, are removed.
    expect(response.getHeaders()).andReturn(httpFields).anyTimes();
    expect(httpFields.remove("FooBarAttr")).andReturn(httpFields);

    replay(request, response, httpFields);

    customLog.log(request, response);
    verify(request, response, httpFields);

    assertEquals("PATCH testUri -", logEntry);
  }
}
