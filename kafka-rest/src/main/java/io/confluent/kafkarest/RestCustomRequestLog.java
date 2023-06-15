package io.confluent.kafkarest;

import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RestCustomRequestLog implements Jetty's RequestLog interface. It offers the same log-format
 * jetty's CustomRequestLog. Additionally, it would append configured request-attributes (see
 * requestAttributesToLog) to the end of the log.
 */
public class RestCustomRequestLog implements RequestLog {

  private static final Logger log = LoggerFactory.getLogger(RestCustomRequestLog.class);

  private RequestLog jettyRequestLog;

  private String[] requestAttributesToLog;

  public RestCustomRequestLog(
      RequestLog.Writer writer, String formatString, String[] requestAttributesToLog) {
    for (String attr : requestAttributesToLog) {
      formatString += " %{" + attr + "}o";
    }
    this.requestAttributesToLog = requestAttributesToLog;
    this.jettyRequestLog = new CustomRequestLog(writer, formatString);
  }

  @Override
  public void log(Request request, Response response) {
    // Add request-attributes to log as response-headers. Then these response-headers are logged
    // in jetty's CustomRequestLog with format specifier "%{AttributeName}o".
    for (String attr : this.requestAttributesToLog) {
      Object attrVal = request.getAttribute(attr);
      if (attrVal != null) {
        request.removeAttribute(attr);
        response.setHeader(attr, attrVal.toString());
      }
    }

    jettyRequestLog.log(request, response);

    // Remove the response-headers that were added above just for logging.
    for (String attr : this.requestAttributesToLog) {
      response.getHttpFields().remove(attr);
    }
  }
}
