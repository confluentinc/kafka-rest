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

package io.confluent.kafkarest;

import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.foldArguments;
import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.Cookie;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.QuotedCSV;
import org.eclipse.jetty.http.pathmap.PathMappings;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.RequestLogWriter;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.DateCache;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.eclipse.jetty.util.annotation.ManagedObject;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/*
 * Below class is "mostly" a copy of (jetty/9.4.x), CustomRequestLog.java.
 * https://github.com/eclipse/jetty.project/blob/ab864a1650e91337b02e19b25efe72e245260035/jetty-server/src/main/java/org/eclipse/jetty/server/CustomRequestLog.java#L273
 * The only additional logic is for request-attribute-logging, see log() &
 * addRequestAttributeToLog().
 *
 * NOTE - it wasn't possible to extend CustomRequestLog from Jetty, hence the copy.
 * The CustomRequestLog.log() method does both 1. create string representation of the request 2.
 * and log it the log-writer/destination, in "one shot". For REST's own custom-logging, we want to
 * retain 1., and extend it some "extra" information. But log() method isn't setup for that.
 */

@ManagedObject("Custom format request log")
public class RestCustomRequestLog extends ContainerLifeCycle implements RequestLog {
  protected static final Logger LOG = Log.getLogger(RestCustomRequestLog.class);

  public static final String DEFAULT_DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss ZZZ";
  public static final String NCSA_FORMAT = "%{client}a - %u %t \"%r\" %s %O";
  public static final String EXTENDED_NCSA_FORMAT =
      NCSA_FORMAT + " \"%{Referer}i\" \"%{User-Agent}i\"";
  private static final ThreadLocal<StringBuilder> _buffers =
      ThreadLocal.withInitial(() -> new StringBuilder(256));

  private final RequestLog.Writer _requestLogWriter;
  private final MethodHandle _logHandle;
  private final String _formatString;
  private transient PathMappings<String> _ignorePathMap;
  private String[] _ignorePaths;

  private String[] _requestAttributesToLog;

  public RestCustomRequestLog(RequestLog.Writer writer, String formatString) {
    _formatString = formatString;
    _requestLogWriter = writer;
    addBean(_requestLogWriter);

    try {
      _logHandle = getLogHandle(formatString);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  public RestCustomRequestLog(String file) {
    this(file, EXTENDED_NCSA_FORMAT);
  }

  public RestCustomRequestLog(String file, String format) {
    this(new RequestLogWriter(file), format);
  }

  @ManagedAttribute("The RequestLogWriter")
  public RequestLog.Writer getWriter() {
    return _requestLogWriter;
  }

  private void addRequestAttributeToLog(Request request, StringBuilder logBuilder) {
    if (_requestAttributesToLog == null || _requestAttributesToLog.length == 0) {
      return;
    }
    for (String attr : this._requestAttributesToLog) {
      Object attrVal = request.getAttribute(attr);
      if (attrVal == null) {
        continue;
      }
      logBuilder.append(" ");
      logBuilder.append(attrVal);
    }
  }

  /**
   * Setter for request-attributes that will be logged.
   *
   * @param attributes array request-attributes.
   */
  public void setRequestAttributesToLog(String[] attributes) {
    _requestAttributesToLog = attributes;
  }

  /**
   * Writes the request and response information to the output stream. This will also log
   * request-attributes configured to be logged in _requestAttributesToLog.
   *
   * @see org.eclipse.jetty.server.RequestLog#log(Request, Response)
   */
  @Override
  public void log(Request request, Response response) {
    try {
      if (_ignorePathMap != null && _ignorePathMap.getMatched(request.getRequestURI()) != null) {
        return;
      }

      StringBuilder sb = _buffers.get();
      sb.setLength(0);

      _logHandle.invoke(sb, request, response);
      addRequestAttributeToLog(request, sb);
      String log = sb.toString();
      _requestLogWriter.write(log);
    } catch (Throwable e) {
      LOG.warn(e);
    }
  }

  /**
   * Extract the user authentication
   *
   * @param request The request to extract from
   * @param checkDeferred Whether to check for deferred authentication
   * @return The string to log for authenticated user.
   */
  protected static String getAuthentication(Request request, boolean checkDeferred) {
    Authentication authentication = request.getAuthentication();
    if (checkDeferred && authentication instanceof Authentication.Deferred) {
      authentication = ((Authentication.Deferred) authentication).authenticate(request);
    }

    String name = null;
    if (authentication instanceof Authentication.User) {
      name = ((Authentication.User) authentication).getUserIdentity().getUserPrincipal().getName();
    }

    return name;
  }

  /**
   * Set request paths that will not be logged.
   *
   * @param ignorePaths array of request paths
   */
  public void setIgnorePaths(String[] ignorePaths) {
    _ignorePaths = ignorePaths;
  }

  /**
   * Retrieve the request paths that will not be logged.
   *
   * @return array of request paths
   */
  public String[] getIgnorePaths() {
    return _ignorePaths;
  }

  /**
   * Retrieve the format string.
   *
   * @return the format string
   */
  @ManagedAttribute("format string")
  public String getFormatString() {
    return _formatString;
  }

  /**
   * Set up request logging and open log file.
   *
   * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStart()
   */
  @Override
  protected synchronized void doStart() throws Exception {
    if (_ignorePaths != null && _ignorePaths.length > 0) {
      _ignorePathMap = new PathMappings<>();
      for (String ignorePath : _ignorePaths) {
        _ignorePathMap.put(ignorePath, ignorePath);
      }
    } else {
      _ignorePathMap = null;
    }

    super.doStart();
  }

  private static void append(StringBuilder buf, String s) {
    if (s == null || s.length() == 0) {
      buf.append('-');
    } else {
      buf.append(s);
    }
  }

  private static void append(String s, StringBuilder buf) {
    append(buf, s);
  }

  private MethodHandle getLogHandle(String formatString)
      throws NoSuchMethodException, IllegalAccessException {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodHandle append =
        lookup.findStatic(
            RestCustomRequestLog.class,
            "append",
            methodType(void.class, String.class, StringBuilder.class));
    MethodHandle logHandle =
        lookup.findStatic(
            RestCustomRequestLog.class,
            "logNothing",
            methodType(void.class, StringBuilder.class, Request.class, Response.class));

    List<Token> tokens = getTokens(formatString);
    Collections.reverse(tokens);

    for (Token t : tokens) {
      if (t.isLiteralString()) {
        logHandle = updateLogHandle(logHandle, append, t.literal);
      } else if (t.isPercentCode()) {
        logHandle =
            updateLogHandle(logHandle, append, lookup, t.code, t.arg, t.modifiers, t.negated);
      } else {
        throw new IllegalStateException("bad token " + t);
      }
    }

    return logHandle;
  }

  private static List<Token> getTokens(String formatString) {
    /*
    Extracts literal strings and percent codes out of the format string.
    We will either match a percent code of the format %MODIFIERS{PARAM}CODE, or a literal string
    until the next percent code or the end of the formatString is reached.

    where
        MODIFIERS is an optional comma separated list of numbers.
        {PARAM} is an optional string parameter to the percent code.
        CODE is a 1 to 2 character string corresponding to a format code.
     */
    final Pattern PATTERN =
        Pattern.compile(
            "^(?:%(?<MOD>!?[0-9,]+)?(?:\\{(?<ARG>[^}]+)})?(?<CODE>(?:(?:ti)|(?:to)|[a-zA-Z%]))|(?<LITERAL>[^%]+))(?<REMAINING>.*)",
            Pattern.DOTALL | Pattern.MULTILINE);

    List<Token> tokens = new ArrayList<>();
    String remaining = formatString;
    while (remaining.length() > 0) {
      Matcher m = PATTERN.matcher(remaining);
      if (m.matches()) {
        if (m.group("CODE") != null) {
          String code = m.group("CODE");
          String arg = m.group("ARG");
          String modifierString = m.group("MOD");

          List<Integer> modifiers = null;
          boolean negated = false;
          if (modifierString != null) {
            if (modifierString.startsWith("!")) {
              modifierString = modifierString.substring(1);
              negated = true;
            }

            modifiers =
                new QuotedCSV(modifierString)
                    .getValues().stream().map(Integer::parseInt).collect(Collectors.toList());
          }

          tokens.add(new Token(code, arg, modifiers, negated));
        } else if (m.group("LITERAL") != null) {
          String literal = m.group("LITERAL");
          tokens.add(new Token(literal));
        } else {
          throw new IllegalStateException("formatString parsing error");
        }

        remaining = m.group("REMAINING");
      } else {
        throw new IllegalArgumentException("Invalid format string");
      }
    }

    return tokens;
  }

  private static class Token {

    public final String code;
    public final String arg;
    public final List<Integer> modifiers;
    public final boolean negated;

    public final String literal;

    public Token(String code, String arg, List<Integer> modifiers, boolean negated) {
      this.code = code;
      this.arg = arg;
      this.modifiers = modifiers;
      this.negated = negated;
      this.literal = null;
    }

    public Token(String literal) {
      this.code = null;
      this.arg = null;
      this.modifiers = null;
      this.negated = false;
      this.literal = literal;
    }

    public boolean isLiteralString() {
      return (literal != null);
    }

    public boolean isPercentCode() {
      return (code != null);
    }
  }

  @SuppressWarnings("unused")
  private static boolean modify(
      List<Integer> modifiers,
      Boolean negated,
      StringBuilder b,
      Request request,
      Response response) {
    if (negated) {
      return !modifiers.contains(response.getStatus());
    } else {
      return modifiers.contains(response.getStatus());
    }
  }

  private MethodHandle updateLogHandle(
      MethodHandle logHandle, MethodHandle append, String literal) {
    return foldArguments(
        logHandle,
        dropArguments(dropArguments(append.bindTo(literal), 1, Request.class), 2, Response.class));
  }

  private MethodHandle updateLogHandle(
      MethodHandle logHandle,
      MethodHandle append,
      MethodHandles.Lookup lookup,
      String code,
      String arg,
      List<Integer> modifiers,
      boolean negated)
      throws NoSuchMethodException, IllegalAccessException {
    MethodType logType = methodType(void.class, StringBuilder.class, Request.class, Response.class);
    MethodType logTypeArg =
        methodType(void.class, String.class, StringBuilder.class, Request.class, Response.class);

    // TODO should we throw IllegalArgumentExceptions when given arguments for codes which do not
    // take them
    MethodHandle specificHandle;
    switch (code) {
      case "%":
        {
          specificHandle =
              dropArguments(dropArguments(append.bindTo("%"), 1, Request.class), 2, Response.class);
          break;
        }

      case "a":
        {
          if (StringUtil.isEmpty(arg)) {
            arg = "server";
          }

          String method;
          switch (arg) {
            case "server":
              method = "logServerHost";
              break;

            case "client":
              method = "logClientHost";
              break;

            case "local":
              method = "logLocalHost";
              break;

            case "remote":
              method = "logRemoteHost";
              break;

            default:
              throw new IllegalArgumentException("Invalid arg for %a");
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "p":
        {
          if (StringUtil.isEmpty(arg)) {
            arg = "server";
          }

          String method;
          switch (arg) {
            case "server":
              method = "logServerPort";
              break;

            case "client":
              method = "logClientPort";
              break;

            case "local":
              method = "logLocalPort";
              break;

            case "remote":
              method = "logRemotePort";
              break;

            default:
              throw new IllegalArgumentException("Invalid arg for %p");
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "I":
        {
          String method;
          if (StringUtil.isEmpty(arg)) {
            method = "logBytesReceived";
          } else if (arg.equalsIgnoreCase("clf")) {
            method = "logBytesReceivedCLF";
          } else {
            throw new IllegalArgumentException("Invalid argument for %I");
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "O":
        {
          String method;
          if (StringUtil.isEmpty(arg)) {
            method = "logBytesSent";
          } else if (arg.equalsIgnoreCase("clf")) {
            method = "logBytesSentCLF";
          } else {
            throw new IllegalArgumentException("Invalid argument for %O");
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "S":
        {
          String method;
          if (StringUtil.isEmpty(arg)) {
            method = "logBytesTransferred";
          } else if (arg.equalsIgnoreCase("clf")) {
            method = "logBytesTransferredCLF";
          } else {
            throw new IllegalArgumentException("Invalid argument for %S");
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "C":
        {
          if (StringUtil.isEmpty(arg)) {
            specificHandle =
                lookup.findStatic(RestCustomRequestLog.class, "logRequestCookies", logType);
          } else {
            specificHandle =
                lookup.findStatic(RestCustomRequestLog.class, "logRequestCookie", logTypeArg);
            specificHandle = specificHandle.bindTo(arg);
          }
          break;
        }

      case "D":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logLatencyMicroseconds", logType);
          break;
        }

      case "e":
        {
          if (StringUtil.isEmpty(arg)) {
            throw new IllegalArgumentException("No arg for %e");
          }

          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logEnvironmentVar", logTypeArg);
          specificHandle = specificHandle.bindTo(arg);
          break;
        }

      case "f":
        {
          specificHandle = lookup.findStatic(RestCustomRequestLog.class, "logFilename", logType);
          break;
        }

      case "H":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestProtocol", logType);
          break;
        }

      case "i":
        {
          if (StringUtil.isEmpty(arg)) {
            throw new IllegalArgumentException("No arg for %i");
          }

          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestHeader", logTypeArg);
          specificHandle = specificHandle.bindTo(arg);
          break;
        }

      case "k":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logKeepAliveRequests", logType);
          break;
        }

      case "m":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestMethod", logType);
          break;
        }

      case "o":
        {
          if (StringUtil.isEmpty(arg)) {
            throw new IllegalArgumentException("No arg for %o");
          }

          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logResponseHeader", logTypeArg);
          specificHandle = specificHandle.bindTo(arg);
          break;
        }

      case "q":
        {
          specificHandle = lookup.findStatic(RestCustomRequestLog.class, "logQueryString", logType);
          break;
        }

      case "r":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestFirstLine", logType);
          break;
        }

      case "R":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestHandler", logType);
          break;
        }

      case "s":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logResponseStatus", logType);
          break;
        }

      case "t":
        {
          String format = DEFAULT_DATE_FORMAT;
          TimeZone timeZone = TimeZone.getTimeZone("GMT");
          Locale locale = Locale.getDefault();

          if (arg != null && !arg.isEmpty()) {
            String[] args = arg.split("\\|");
            switch (args.length) {
              case 1:
                format = args[0];
                break;

              case 2:
                format = args[0];
                timeZone = TimeZone.getTimeZone(args[1]);
                break;

              case 3:
                format = args[0];
                timeZone = TimeZone.getTimeZone(args[1]);
                locale = Locale.forLanguageTag(args[2]);
                break;

              default:
                throw new IllegalArgumentException("Too many \"|\" characters in %t");
            }
          }

          DateCache logDateCache = new DateCache(format, locale, timeZone);

          MethodType logTypeDateCache =
              methodType(
                  void.class, DateCache.class, StringBuilder.class, Request.class, Response.class);
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestTime", logTypeDateCache);
          specificHandle = specificHandle.bindTo(logDateCache);
          break;
        }

      case "T":
        {
          if (arg == null) {
            arg = "s";
          }

          String method;
          switch (arg) {
            case "s":
              method = "logLatencySeconds";
              break;
            case "us":
              method = "logLatencyMicroseconds";
              break;
            case "ms":
              method = "logLatencyMilliseconds";
              break;
            default:
              throw new IllegalArgumentException("Invalid arg for %T");
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "u":
        {
          String method;
          if (StringUtil.isEmpty(arg)) {
            method = "logRequestAuthentication";
          } else if ("d".equals(arg)) {
            method = "logRequestAuthenticationWithDeferred";
          } else {
            throw new IllegalArgumentException("Invalid arg for %u: " + arg);
          }

          specificHandle = lookup.findStatic(RestCustomRequestLog.class, method, logType);
          break;
        }

      case "U":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logUrlRequestPath", logType);
          break;
        }

      case "X":
        {
          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logConnectionStatus", logType);
          break;
        }

      case "ti":
        {
          if (StringUtil.isEmpty(arg)) {
            throw new IllegalArgumentException("No arg for %ti");
          }

          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logRequestTrailer", logTypeArg);
          specificHandle = specificHandle.bindTo(arg);
          break;
        }

      case "to":
        {
          if (StringUtil.isEmpty(arg)) {
            throw new IllegalArgumentException("No arg for %to");
          }

          specificHandle =
              lookup.findStatic(RestCustomRequestLog.class, "logResponseTrailer", logTypeArg);
          specificHandle = specificHandle.bindTo(arg);
          break;
        }

      default:
        throw new IllegalArgumentException("Unsupported code %" + code);
    }

    if (modifiers != null && !modifiers.isEmpty()) {
      MethodHandle dash = updateLogHandle(logHandle, append, "-");
      MethodHandle log = foldArguments(logHandle, specificHandle);

      MethodHandle modifierTest =
          lookup.findStatic(
              RestCustomRequestLog.class,
              "modify",
              methodType(
                  Boolean.TYPE,
                  List.class,
                  Boolean.class,
                  StringBuilder.class,
                  Request.class,
                  Response.class));
      modifierTest = modifierTest.bindTo(modifiers).bindTo(negated);
      return MethodHandles.guardWithTest(modifierTest, log, dash);
    }

    return foldArguments(logHandle, specificHandle);
  }

  // -----------------------------------------------------------------------------------//
  @SuppressWarnings("unused")
  private static void logNothing(StringBuilder b, Request request, Response response) {}

  @SuppressWarnings("unused")
  private static void logServerHost(StringBuilder b, Request request, Response response) {
    append(b, request.getServerName());
  }

  @SuppressWarnings("unused")
  private static void logClientHost(StringBuilder b, Request request, Response response) {
    append(b, request.getRemoteHost());
  }

  @SuppressWarnings("unused")
  private static void logLocalHost(StringBuilder b, Request request, Response response) {
    append(
        b, request.getHttpChannel().getEndPoint().getLocalAddress().getAddress().getHostAddress());
  }

  @SuppressWarnings("unused")
  private static void logRemoteHost(StringBuilder b, Request request, Response response) {
    append(
        b, request.getHttpChannel().getEndPoint().getRemoteAddress().getAddress().getHostAddress());
  }

  @SuppressWarnings("unused")
  private static void logServerPort(StringBuilder b, Request request, Response response) {
    b.append(request.getServerPort());
  }

  @SuppressWarnings("unused")
  private static void logClientPort(StringBuilder b, Request request, Response response) {
    b.append(request.getRemotePort());
  }

  @SuppressWarnings("unused")
  private static void logLocalPort(StringBuilder b, Request request, Response response) {
    b.append(request.getHttpChannel().getEndPoint().getLocalAddress().getPort());
  }

  @SuppressWarnings("unused")
  private static void logRemotePort(StringBuilder b, Request request, Response response) {
    b.append(request.getHttpChannel().getEndPoint().getRemoteAddress().getPort());
  }

  @SuppressWarnings("unused")
  private static void logResponseSize(StringBuilder b, Request request, Response response) {
    long written = response.getHttpChannel().getBytesWritten();
    b.append(written);
  }

  @SuppressWarnings("unused")
  private static void logResponseSizeCLF(StringBuilder b, Request request, Response response) {
    long written = response.getHttpChannel().getBytesWritten();
    if (written == 0) {
      b.append('-');
    } else {
      b.append(written);
    }
  }

  @SuppressWarnings("unused")
  private static void logBytesSent(StringBuilder b, Request request, Response response) {
    b.append(response.getHttpChannel().getBytesWritten());
  }

  @SuppressWarnings("unused")
  private static void logBytesSentCLF(StringBuilder b, Request request, Response response) {
    long sent = response.getHttpChannel().getBytesWritten();
    if (sent == 0) {
      b.append('-');
    } else {
      b.append(sent);
    }
  }

  @SuppressWarnings("unused")
  private static void logBytesReceived(StringBuilder b, Request request, Response response) {
    b.append(request.getHttpInput().getContentReceived());
  }

  @SuppressWarnings("unused")
  private static void logBytesReceivedCLF(StringBuilder b, Request request, Response response) {
    long received = request.getHttpInput().getContentReceived();
    if (received == 0) {
      b.append('-');
    } else {
      b.append(received);
    }
  }

  @SuppressWarnings("unused")
  private static void logBytesTransferred(StringBuilder b, Request request, Response response) {
    b.append(request.getHttpInput().getContentReceived() + response.getHttpOutput().getWritten());
  }

  @SuppressWarnings("unused")
  private static void logBytesTransferredCLF(StringBuilder b, Request request, Response response) {
    long transferred =
        request.getHttpInput().getContentReceived() + response.getHttpOutput().getWritten();
    if (transferred == 0) {
      b.append('-');
    } else {
      b.append(transferred);
    }
  }

  @SuppressWarnings("unused")
  private static void logRequestCookie(
      String arg, StringBuilder b, Request request, Response response) {
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie c : cookies) {
        if (arg.equals(c.getName())) {
          b.append(c.getValue());
          return;
        }
      }
    }

    b.append('-');
  }

  @SuppressWarnings("unused")
  private static void logRequestCookies(StringBuilder b, Request request, Response response) {
    Cookie[] cookies = request.getCookies();
    if (cookies == null || cookies.length == 0) {
      b.append("-");
    } else {
      for (int i = 0; i < cookies.length; i++) {
        if (i != 0) {
          b.append(';');
        }
        b.append(cookies[i].getName());
        b.append('=');
        b.append(cookies[i].getValue());
      }
    }
  }

  @SuppressWarnings("unused")
  private static void logEnvironmentVar(
      String arg, StringBuilder b, Request request, Response response) {
    append(b, System.getenv(arg));
  }

  @SuppressWarnings("unused")
  private static void logFilename(StringBuilder b, Request request, Response response) {
    UserIdentity.Scope scope = request.getUserIdentityScope();
    if (scope == null || scope.getContextHandler() == null) {
      b.append('-');
    } else {
      ContextHandler context = scope.getContextHandler();
      int lengthToStrip = scope.getContextPath().length() > 1 ? scope.getContextPath().length() : 0;
      String filename =
          context.getServletContext().getRealPath(request.getPathInfo().substring(lengthToStrip));
      append(b, filename);
    }
  }

  @SuppressWarnings("unused")
  private static void logRequestProtocol(StringBuilder b, Request request, Response response) {
    append(b, request.getProtocol());
  }

  @SuppressWarnings("unused")
  private static void logRequestHeader(
      String arg, StringBuilder b, Request request, Response response) {
    append(b, request.getHeader(arg));
  }

  @SuppressWarnings("unused")
  private static void logKeepAliveRequests(StringBuilder b, Request request, Response response) {
    long requests = request.getHttpChannel().getConnection().getMessagesIn();
    if (requests >= 0) {
      b.append(requests);
    } else {
      b.append('-');
    }
  }

  @SuppressWarnings("unused")
  private static void logRequestMethod(StringBuilder b, Request request, Response response) {
    append(b, request.getMethod());
  }

  @SuppressWarnings("unused")
  private static void logResponseHeader(
      String arg, StringBuilder b, Request request, Response response) {
    append(b, response.getHeader(arg));
  }

  @SuppressWarnings("unused")
  private static void logQueryString(StringBuilder b, Request request, Response response) {
    append(b, "?" + request.getQueryString());
  }

  @SuppressWarnings("unused")
  private static void logRequestFirstLine(StringBuilder b, Request request, Response response) {
    append(b, request.getMethod());
    b.append(" ");
    append(b, request.getOriginalURI());
    b.append(" ");
    append(b, request.getProtocol());
  }

  @SuppressWarnings("unused")
  private static void logRequestHandler(StringBuilder b, Request request, Response response) {
    append(b, request.getServletName());
  }

  @SuppressWarnings("unused")
  private static void logResponseStatus(StringBuilder b, Request request, Response response) {
    b.append(response.getCommittedMetaData().getStatus());
  }

  @SuppressWarnings("unused")
  private static void logRequestTime(
      DateCache dateCache, StringBuilder b, Request request, Response response) {
    b.append('[');
    append(b, dateCache.format(request.getTimeStamp()));
    b.append(']');
  }

  @SuppressWarnings("unused")
  private static void logLatencyMicroseconds(StringBuilder b, Request request, Response response) {
    long currentTime = System.currentTimeMillis();
    long requestTime = request.getTimeStamp();

    long latencyMs = currentTime - requestTime;
    long latencyUs = TimeUnit.MILLISECONDS.toMicros(latencyMs);

    b.append(latencyUs);
  }

  @SuppressWarnings("unused")
  private static void logLatencyMilliseconds(StringBuilder b, Request request, Response response) {
    long latency = System.currentTimeMillis() - request.getTimeStamp();
    b.append(latency);
  }

  @SuppressWarnings("unused")
  private static void logLatencySeconds(StringBuilder b, Request request, Response response) {
    long latency = System.currentTimeMillis() - request.getTimeStamp();
    b.append(TimeUnit.MILLISECONDS.toSeconds(latency));
  }

  @SuppressWarnings("unused")
  private static void logRequestAuthentication(
      StringBuilder b, Request request, Response response) {
    append(b, getAuthentication(request, false));
  }

  @SuppressWarnings("unused")
  private static void logRequestAuthenticationWithDeferred(
      StringBuilder b, Request request, Response response) {
    append(b, getAuthentication(request, true));
  }

  @SuppressWarnings("unused")
  private static void logUrlRequestPath(StringBuilder b, Request request, Response response) {
    append(b, request.getRequestURI());
  }

  @SuppressWarnings("unused")
  private static void logConnectionStatus(StringBuilder b, Request request, Response response) {
    b.append(
        request.getHttpChannel().isResponseCompleted()
            ? (request.getHttpChannel().isPersistent() ? '+' : '-')
            : 'X');
  }

  @SuppressWarnings("unused")
  private static void logRequestTrailer(
      String arg, StringBuilder b, Request request, Response response) {
    HttpFields trailers = request.getTrailers();
    if (trailers != null) {
      append(b, trailers.get(arg));
    } else {
      b.append('-');
    }
  }

  @SuppressWarnings("unused")
  private static void logResponseTrailer(
      String arg, StringBuilder b, Request request, Response response) {
    Supplier<HttpFields> supplier = response.getTrailers();
    if (supplier != null) {
      HttpFields trailers = supplier.get();

      if (trailers != null) {
        append(b, trailers.get(arg));
      } else {
        b.append('-');
      }
    } else {
      b.append("-");
    }
  }
}
