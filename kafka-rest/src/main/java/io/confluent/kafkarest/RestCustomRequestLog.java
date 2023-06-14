package io.confluent.kafkarest;

import java.io.IOException;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RestCustomRequestLog implements Jetty's RequestLog interface. It offers the same log-format
 * jetty's CustomRequestLog. Additionally it would append configured request-attributes (see
 * requestAttributesToLog) to the end of the log.
 */

public class RestCustomRequestLog implements RequestLog {

  private static final Logger log = LoggerFactory.getLogger(RestCustomRequestLog.class);

  private RequestLog.Writer logWriter;
  private String formatString;

  private String[] requestAttributesToLog;

  public RestCustomRequestLog(
      RequestLog.Writer writer, String formatString, String[] requestAttributesToLog) {
    this.logWriter = writer;
    this.formatString = formatString;
    this.requestAttributesToLog = requestAttributesToLog;
  }

  @Override
  public void log(final Request request, final Response response) {
    try {
      StringBuilder logStringBuilder = new StringBuilder();
      RequestLog.Writer jettyLogWriter = new StringBuilderLogWriter(logStringBuilder);
      RequestLog jettyLog = new CustomRequestLog(jettyLogWriter, formatString);
      jettyLog.log(request, response);
      addRequestAttributeToLog(request, logStringBuilder);
      logWriter.write(logStringBuilder.toString());
    } catch (Exception e) {
      log.error("Failed to log request with exception {} with stack \n {}", e, e.getStackTrace());
    }
  }

  private void addRequestAttributeToLog(Request request, StringBuilder logBuilder) {
    if (requestAttributesToLog == null || requestAttributesToLog.length == 0) {
      return;
    }
    for (String attr : this.requestAttributesToLog) {
      Object attrVal = request.getAttribute(attr);
      if (attrVal == null) {
        continue;
      }
      logBuilder.append(" ");
      logBuilder.append(attrVal);
    }
  }

  private class StringBuilderLogWriter implements RequestLog.Writer {

    private StringBuilder logBuilder;

    StringBuilderLogWriter(StringBuilder logBuilder) {
      this.logBuilder = logBuilder;
    }

    @Override
    public void write(final String s) throws IOException {
      logBuilder.append(s);
    }
  }
}
