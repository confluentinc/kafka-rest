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

package io.confluent.kafkarest.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.RestCustomRequestLog;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.QuietServletException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.DateCache;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Below class is "mostly" taken as it from (jetty/9.4.x), CustomRequestLogTest.java, except for the
 * tests to validate "request-attribute-logging" and they start with the prefix
 * "testRequestAttributeLogging", at the bottom of the file. Original source for
 * CustomRequestLogTest.java -
 * https://github.com/eclipse/jetty.project/blob/30845556878cb9b8e92eed905f69600249ca50fe/tests/test-integration/src/test/java/org/eclipse/jetty/test/CustomRequestLogTest.java#L19
 */
@Tag("IntegrationTest")
public class RestCustomRequestLogTest {

  RestCustomRequestLog _log;
  Server _server;
  LocalConnector _connector;
  BlockingQueue<String> _entries = new BlockingArrayQueue<>();
  BlockingQueue<Long> requestTimes = new BlockingArrayQueue<>();
  ServerConnector _serverConnector;
  URI _serverURI;

  private static final long DELAY = 2000;

  @BeforeEach
  public void before() throws Exception {
    _server = new Server();
    _connector = new LocalConnector(_server);
    _serverConnector = new ServerConnector(_server);
    _server.addConnector(_connector);
    _server.addConnector(_serverConnector);
  }

  void testHandlerServerStart(String formatString) throws Exception {
    _serverConnector.setPort(0);
    _serverConnector
        .getBean(HttpConnectionFactory.class)
        .getHttpConfiguration()
        .addCustomizer(new ForwardedRequestCustomizer());
    TestRequestLogWriter writer = new TestRequestLogWriter();
    _log = new RestCustomRequestLog(writer, formatString);
    _server.setRequestLog(_log);
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setSecurityHandler(getSecurityHandler("username", "password", "testRealm"));
    contextHandler.addServlet(new ServletHolder(new TestServlet()), "/");
    _server.setHandler(contextHandler);
    _server.start();

    String host = _serverConnector.getHost();
    if (host == null) {
      host = "localhost";
    }

    int localPort = _serverConnector.getLocalPort();
    _serverURI = new URI(String.format("http://%s:%d/", host, localPort));
  }

  private static SecurityHandler getSecurityHandler(
      String username, String password, String realm) {
    HashLoginService loginService = new HashLoginService();
    UserStore userStore = new UserStore();
    userStore.addUser(username, Credential.getCredential(password), new String[] {"user"});
    loginService.setUserStore(userStore);
    loginService.setName(realm);

    Constraint constraint = new Constraint();
    constraint.setName("auth");
    constraint.setAuthenticate(true);
    constraint.setRoles(new String[] {"**"});

    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setPathSpec("/secure/*");
    mapping.setConstraint(constraint);

    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    security.addConstraintMapping(mapping);
    security.setAuthenticator(new BasicAuthenticator());
    security.setLoginService(loginService);

    return security;
  }

  @AfterEach
  public void after() throws Exception {
    _server.stop();
  }

  @Test
  public void testLogRemoteUser() throws Exception {
    String authHeader =
        HttpHeader.AUTHORIZATION
            + ": Basic "
            + Base64.getEncoder().encodeToString("username:password".getBytes());
    testHandlerServerStart("%u %{d}u");

    _connector.getResponse("GET / HTTP/1.0\n\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("- -"));

    _connector.getResponse("GET / HTTP/1.0\n" + authHeader + "\n\n\n");
    log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("- username"));

    _connector.getResponse("GET /secure HTTP/1.0\n" + authHeader + "\n\n\n");
    log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("username username"));
  }

  @Test
  public void testModifier() throws Exception {
    testHandlerServerStart("%s: %!404,301{Referer}i");

    _connector.getResponse("GET /error404 HTTP/1.0\nReferer: testReferer\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("404: -"));

    _connector.getResponse("GET /error301 HTTP/1.0\nReferer: testReferer\n\n");
    log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("301: -"));

    _connector.getResponse("GET /success HTTP/1.0\nReferer: testReferer\n\n");
    log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("200: testReferer"));
  }

  @Test
  public void testDoublePercent() throws Exception {
    testHandlerServerStart("%%%%%%a");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("%%%a"));
  }

  @Test
  public void testLogAddress() throws Exception {
    testHandlerServerStart(
        "%{local}a|%{local}p|"
            + "%{remote}a|%{remote}p|"
            + "%{server}a|%{server}p|"
            + "%{client}a|%{client}p");

    Enumeration e = NetworkInterface.getNetworkInterfaces();
    while (e.hasMoreElements()) {
      NetworkInterface n = (NetworkInterface) e.nextElement();
      if (n.isLoopback()) {
        Enumeration ee = n.getInetAddresses();
        while (ee.hasMoreElements()) {
          InetAddress i = (InetAddress) ee.nextElement();
          try (Socket client = newSocket(i.getHostAddress(), _serverURI.getPort())) {
            OutputStream os = client.getOutputStream();
            String request =
                "GET / HTTP/1.0\n"
                    + "Host: webtide.com:1234\n"
                    + "Forwarded: For=10.1.2.3:1337\n"
                    + "\n\n";
            os.write(request.getBytes(StandardCharsets.ISO_8859_1));
            os.flush();

            String[] log = _entries.poll(5, TimeUnit.SECONDS).split("\\|");
            assertThat(log.length, is(8));

            String localAddr = log[0];
            String localPort = log[1];
            String remoteAddr = log[2];
            String remotePort = log[3];
            String serverAddr = log[4];
            String serverPort = log[5];
            String clientAddr = log[6];
            String clientPort = log[7];

            assertThat(serverPort, is("1234"));
            assertThat(clientPort, is("1337"));
            assertThat(remotePort, not(clientPort));
            assertThat(localPort, not(serverPort));

            assertThat(serverAddr, is("webtide.com"));
            assertThat(clientAddr, is("10.1.2.3"));
            assertThat(InetAddress.getByName(remoteAddr), is(client.getInetAddress()));
            assertThat(InetAddress.getByName(localAddr), is(i));
          }
        }
      }
    }
  }

  @Test
  public void testLogBytesSent() throws Exception {
    testHandlerServerStart("BytesSent: %O");

    _connector.getResponse("GET / HTTP/1.0\necho: hello world\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("BytesSent: 11"));
  }

  @Test
  public void testLogBytesReceived() throws Exception {
    testHandlerServerStart("BytesReceived: %I");

    _connector.getResponse("GET / HTTP/1.0\n" + "Content-Length: 11\n\n" + "hello world");

    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("BytesReceived: 11"));
  }

  @Test
  public void testLogBytesTransferred() throws Exception {
    testHandlerServerStart("BytesTransferred: %S");

    _connector.getResponse(
        "GET / HTTP/1.0\n" + "echo: hello world\n" + "Content-Length: 11\n\n" + "hello world");

    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("BytesTransferred: 22"));
  }

  @Test
  public void testLogRequestCookie() throws Exception {
    testHandlerServerStart("RequestCookies: %{cookieName}C, %{cookie2}C, %{cookie3}C");

    _connector.getResponse("GET / HTTP/1.0\nCookie: cookieName=cookieValue; cookie2=value2\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("RequestCookies: cookieValue, value2, -"));
  }

  @Test
  public void testLogRequestCookies() throws Exception {
    testHandlerServerStart("RequestCookies: %C");

    _connector.getResponse("GET / HTTP/1.0\nCookie: cookieName=cookieValue; cookie2=value2\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("RequestCookies: cookieName=cookieValue;cookie2=value2"));
  }

  @Test
  public void testLogEnvironmentVar() throws Exception {
    testHandlerServerStart("EnvironmentVar: %{JAVA_HOME}e");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);

    String envVar = System.getenv("JAVA_HOME");
    assertThat(log, is("EnvironmentVar: " + ((envVar == null) ? "-" : envVar)));
  }

  @Test
  public void testLogRequestProtocol() throws Exception {
    testHandlerServerStart("%H");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("HTTP/1.0"));
  }

  @Test
  public void testLogRequestHeader() throws Exception {
    testHandlerServerStart("RequestHeader: %{Header1}i, %{Header2}i, %{Header3}i");

    _connector.getResponse("GET / HTTP/1.0\nHeader1: value1\nHeader2: value2\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("RequestHeader: value1, value2, -"));
  }

  @Test
  public void testLogKeepAliveRequests() throws Exception {
    testHandlerServerStart("KeepAliveRequests: %k");

    LocalConnector.LocalEndPoint connect = _connector.connect();
    connect.addInput("GET /a HTTP/1.0\n" + "Connection: keep-alive\n\n");
    connect.addInput("GET /a HTTP/1.1\n" + "Host: localhost\n\n");

    assertThat(connect.getResponse(), containsString("200 OK"));
    assertThat(connect.getResponse(), containsString("200 OK"));

    connect.addInput("GET /a HTTP/1.0\n\n");
    assertThat(connect.getResponse(), containsString("200 OK"));

    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("KeepAliveRequests: 1"));
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("KeepAliveRequests: 2"));
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("KeepAliveRequests: 3"));
  }

  @Disabled
  @Test
  public void testLogKeepAliveRequestsHttp2() throws Exception {
    testHandlerServerStart("KeepAliveRequests: %k");
    fail();
  }

  @Test
  public void testLogRequestMethod() throws Exception {
    testHandlerServerStart("RequestMethod: %m");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("RequestMethod: GET"));
  }

  @Test
  public void testLogResponseHeader() throws Exception {
    testHandlerServerStart("ResponseHeader: %{Header1}o, %{Header2}o, %{Header3}o");

    _connector.getResponse("GET /responseHeaders HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("ResponseHeader: value1, value2, -"));
  }

  @Test
  public void testLogQueryString() throws Exception {
    testHandlerServerStart("QueryString: %q");

    _connector.getResponse("GET /path?queryString HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("QueryString: ?queryString"));
  }

  @Test
  public void testLogRequestFirstLine() throws Exception {
    testHandlerServerStart("RequestFirstLin: %r");

    _connector.getResponse("GET /path?query HTTP/1.0\nHeader: null\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("RequestFirstLin: GET /path?query HTTP/1.0"));
  }

  @Test
  public void testLogResponseStatus() throws Exception {
    testHandlerServerStart("LogResponseStatus: %s");

    _connector.getResponse("GET /error404 HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("LogResponseStatus: 404"));

    _connector.getResponse("GET /error301 HTTP/1.0\n\n");
    log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("LogResponseStatus: 301"));

    _connector.getResponse("GET / HTTP/1.0\n\n");
    log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("LogResponseStatus: 200"));
  }

  @Test
  public void testLogRequestTime() throws Exception {
    testHandlerServerStart("RequestTime: %t");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    long requestTime = requestTimes.poll(5, TimeUnit.SECONDS);
    DateCache dateCache =
        new DateCache(RestCustomRequestLog.DEFAULT_DATE_FORMAT, Locale.getDefault(), "GMT");
    assertThat(log, is("RequestTime: [" + dateCache.format(requestTime) + "]"));
  }

  @Test
  public void testLogRequestTimeCustomFormats() throws Exception {
    testHandlerServerStart(
        "%{EEE MMM dd HH:mm:ss zzz yyyy}t\n"
            + "%{EEE MMM dd HH:mm:ss zzz yyyy|EST}t\n"
            + "%{EEE MMM dd HH:mm:ss zzz yyyy|EST|ja}t");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    long requestTime = requestTimes.poll(5, TimeUnit.SECONDS);

    DateCache dateCache1 =
        new DateCache("EEE MMM dd HH:mm:ss zzz yyyy", Locale.getDefault(), "GMT");
    DateCache dateCache2 =
        new DateCache("EEE MMM dd HH:mm:ss zzz yyyy", Locale.getDefault(), "EST");
    DateCache dateCache3 =
        new DateCache("EEE MMM dd HH:mm:ss zzz yyyy", Locale.forLanguageTag("ja"), "EST");

    String[] logs = log.split("\n");
    assertThat(logs[0], is("[" + dateCache1.format(requestTime) + "]"));
    assertThat(logs[1], is("[" + dateCache2.format(requestTime) + "]"));
    assertThat(logs[2], is("[" + dateCache3.format(requestTime) + "]"));
  }

  @Test
  public void testLogLatencyMicroseconds() throws Exception {
    testHandlerServerStart("%{us}T");

    _connector.getResponse("GET /delay HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    long lowerBound = requestTimes.poll(5, TimeUnit.SECONDS);
    long upperBound = System.currentTimeMillis();

    long measuredDuration = Long.parseLong(log);
    long durationLowerBound = TimeUnit.MILLISECONDS.toMicros(DELAY);
    long durationUpperBound = TimeUnit.MILLISECONDS.toMicros(upperBound - lowerBound);

    assertThat(measuredDuration, greaterThanOrEqualTo(durationLowerBound));
    assertThat(measuredDuration, lessThanOrEqualTo(durationUpperBound));
  }

  @Test
  public void testLogLatencyMilliseconds() throws Exception {
    testHandlerServerStart("%{ms}T");

    _connector.getResponse("GET /delay HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    long lowerBound = requestTimes.poll(5, TimeUnit.SECONDS);
    long upperBound = System.currentTimeMillis();

    long measuredDuration = Long.parseLong(log);
    long durationLowerBound = DELAY;
    long durationUpperBound = upperBound - lowerBound;

    assertThat(measuredDuration, greaterThanOrEqualTo(durationLowerBound));
    assertThat(measuredDuration, lessThanOrEqualTo(durationUpperBound));
  }

  @Test
  public void testLogLatencySeconds() throws Exception {
    testHandlerServerStart("%{s}T");

    _connector.getResponse("GET /delay HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    long lowerBound = requestTimes.poll(5, TimeUnit.SECONDS);
    long upperBound = System.currentTimeMillis();

    long measuredDuration = Long.parseLong(log);
    long durationLowerBound = TimeUnit.MILLISECONDS.toSeconds(DELAY);
    long durationUpperBound = TimeUnit.MILLISECONDS.toSeconds(upperBound - lowerBound);

    assertThat(measuredDuration, greaterThanOrEqualTo(durationLowerBound));
    assertThat(measuredDuration, lessThanOrEqualTo(durationUpperBound));
  }

  @Test
  public void testLogUrlRequestPath() throws Exception {
    testHandlerServerStart("UrlRequestPath: %U");

    _connector.getResponse("GET /path?query HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    assertThat(log, is("UrlRequestPath: /path"));
  }

  @Test
  public void testLogConnectionStatus() throws Exception {
    testHandlerServerStart("%U ConnectionStatus: %s %X");

    _connector.getResponse("GET /one HTTP/1.0\n\n");
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/one ConnectionStatus: 200 -"));

    _connector.getResponse(
        "GET /two HTTP/1.1\n" + "Host: localhost\n" + "Connection: close\n" + "\n");
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/two ConnectionStatus: 200 -"));

    LocalConnector.LocalEndPoint connect = _connector.connect();
    connect.addInput("GET /three HTTP/1.0\n" + "Connection: keep-alive\n\n");
    connect.addInput("GET /four HTTP/1.1\n" + "Host: localhost\n\n");
    connect.addInput("GET /BAD HTTP/1.1\n\n");
    assertThat(connect.getResponse(), containsString("200 OK"));
    assertThat(connect.getResponse(), containsString("200 OK"));
    assertThat(connect.getResponse(), containsString("400 "));

    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/three ConnectionStatus: 200 +"));
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/four ConnectionStatus: 200 +"));
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/BAD ConnectionStatus: 400 -"));

    _connector.getResponse("GET /abort HTTP/1.1\n" + "Host: localhost\n" + "\n");
    connect.getResponse();
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/abort ConnectionStatus: 200 X"));
  }

  @Disabled
  @Test
  public void testLogRequestTrailer() throws Exception {
    testHandlerServerStart("%{trailerName}ti");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    fail(log);
  }

  @Disabled
  @Test
  public void testLogResponseTrailer() throws Exception {
    testHandlerServerStart("%{trailerName}to");

    _connector.getResponse("GET / HTTP/1.0\n\n");
    String log = _entries.poll(5, TimeUnit.SECONDS);
    fail(log);
  }

  protected Socket newSocket() throws Exception {
    return newSocket(_serverURI.getHost(), _serverURI.getPort());
  }

  protected Socket newSocket(String host, int port) throws Exception {
    Socket socket = new Socket(host, port);
    socket.setSoTimeout(10000);
    socket.setTcpNoDelay(true);
    return socket;
  }

  class TestRequestLogWriter implements RequestLog.Writer {

    @Override
    public void write(String requestEntry) {
      try {
        _entries.add(requestEntry);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      Request baseRequest = Objects.requireNonNull(Request.getBaseRequest(request));

      if (request.getRequestURI().contains("error404")) {
        response.setStatus(404);
      } else if (request.getRequestURI().contains("error301")) {
        response.setStatus(301);
      } else if (request.getHeader("echo") != null) {
        ServletOutputStream outputStream = response.getOutputStream();
        outputStream.print(request.getHeader("echo"));
      } else if (request.getRequestURI().contains("responseHeaders")) {
        response.addHeader("Header1", "value1");
        response.addHeader("Header2", "value2");
      } else if (request.getRequestURI().contains("/abort")) {
        response.getOutputStream().println("data");
        response.flushBuffer();
        baseRequest.getHttpChannel().abort(new QuietServletException("test abort"));
      } else if (request.getRequestURI().contains("delay")) {
        try {
          Thread.sleep(DELAY);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      requestTimes.offer(baseRequest.getTimeStamp());
      baseRequest.setHandled(true);

      if (request.getContentLength() > 0) {
        InputStream in = request.getInputStream();
        while (in.read() > 0) ;
      }
    }
  }

  /** Below section contains tests & associated helpers for logging request-attributes. */
  private final String testRequestAttribute = "test_attribute";

  private final String testRequestAttributeValue = "test_attribute_value";

  private class TestServletForRequestAttributeLogging extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
      Request baseRequest = Objects.requireNonNull(Request.getBaseRequest(request));
      request.setAttribute(testRequestAttribute, testRequestAttributeValue);

      requestTimes.offer(baseRequest.getTimeStamp());
      baseRequest.setHandled(true);
    }
  }

  void testHandlerServerStartForRequestAttributeLogging(
      String formatString, String requestAttrToLog) throws Exception {
    _serverConnector.setPort(0);
    _serverConnector
        .getBean(HttpConnectionFactory.class)
        .getHttpConfiguration()
        .addCustomizer(new ForwardedRequestCustomizer());
    TestRequestLogWriter writer = new TestRequestLogWriter();
    _log = new RestCustomRequestLog(writer, formatString);
    _log.setRequestAttributesToLog(new String[] {requestAttrToLog});
    _server.setRequestLog(_log);
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setSecurityHandler(getSecurityHandler("username", "password", "testRealm"));
    contextHandler.addServlet(new ServletHolder(new TestServletForRequestAttributeLogging()), "/");
    _server.setHandler(contextHandler);
    _server.start();

    String host = _serverConnector.getHost();
    if (host == null) {
      host = "localhost";
    }

    int localPort = _serverConnector.getLocalPort();
    _serverURI = new URI(String.format("http://%s:%d/", host, localPort));
  }

  @Test
  public void testRequestAttributeLogging_IfAttributeConfigured_ThenLogged() throws Exception {
    testHandlerServerStartForRequestAttributeLogging(
        "%U ConnectionStatus: %s %X", testRequestAttribute);

    _connector.getResponse("GET /one HTTP/1.0\n\n");
    assertThat(
        _entries.poll(5, TimeUnit.SECONDS),
        is("/one ConnectionStatus: 200 - " + testRequestAttributeValue));
  }

  @Test
  public void testRequestAttributeLogging_IfAttributeNotConfigured_ThenNotLogged()
      throws Exception {
    testHandlerServerStartForRequestAttributeLogging(
        "%U ConnectionStatus: %s %X", "test_attribute_invalid");

    _connector.getResponse("GET /one HTTP/1.0\n\n");
    assertThat(_entries.poll(5, TimeUnit.SECONDS), is("/one ConnectionStatus: 200 -"));
  }
}
