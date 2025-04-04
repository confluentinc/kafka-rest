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
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.requestlog;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CustomLog implements Jetty's RequestLog interface. It offers the same log-format jetty's
 * CustomRequestLog. Additionally, it would append configured request-attributes (see
 * requestAttributesToLog) to the end of the log. NOTE - this needs to extend
 * AbstractLifeCycle(implicitly implement LifeCycle) to correctly initialise CustomRequestLog which
 * implements LifeCycle. This in turn will correctly initialise the input RequestLog.Writer, if it
 * also implements LifeCycle.
 */
public class CustomLog extends AbstractLifeCycle implements RequestLog {

  private static final Logger log = LoggerFactory.getLogger(CustomLog.class);

  private final CustomRequestLog delegateJettyLog;

  private final String[] requestAttributesToLog;

  public static final String PRODUCE_ERROR_CODE_LOG_PREFIX = "Codes=";

  public CustomLog(RequestLog.Writer writer, String formatString, String[] requestAttributesToLog) {
    for (String attr : requestAttributesToLog) {
      // Add format-specifier to log request-attributes as response-headers in Jetty's
      // CustomRequestLog.
      formatString += " %{" + attr + "}attr";
    }
    this.requestAttributesToLog = requestAttributesToLog;
    this.delegateJettyLog = new CustomRequestLog(writer, formatString);
  }

  @Override
  protected synchronized void doStart() throws Exception {
    if (this.delegateJettyLog != null) {
      this.delegateJettyLog.start();
    }
  }

  @Override
  protected void doStop() throws Exception {
    if (this.delegateJettyLog != null) {
      this.delegateJettyLog.stop();
    }
  }

  /**
   * This class aggregates error-codes for produce-records within a single (http)produce-request.
   * This implements toString() method which is used by CustomLog to get a message to log for the
   * aggregated error counts.
   */
  public static class ProduceRecordErrorCounter {
    private final Map<Integer, Integer> produceErrorCodeCountMap = new TreeMap<>();

    public synchronized void incrementErrorCount(int httpErrorCode) {
      produceErrorCodeCountMap.merge(httpErrorCode, 1, Integer::sum);
    }

    @Override
    public synchronized String toString() {
      return PRODUCE_ERROR_CODE_LOG_PREFIX
          + produceErrorCodeCountMap.entrySet().stream()
              .map(entry -> entry.getKey() + ":" + entry.getValue())
              .collect(Collectors.joining(","));
    }
  }

  @Override
  public void log(Request request, Response response) {
    // The configured request-attributes are converted to response-headers so Jetty can log them.
    // Request-attributes are chosen to propagate custom-info to the request-log, as
    // 1. Its idiomatic as per ServletRequest(which is implemented by Jetty's request).
    // 2. Places like dosfilter-listeners, ex - GlobalJettyDosFilterListener, only request is
    //    readily available(Vs response).
    // Unfortunately Jetty doesn't provide a way to log request-attributes, hence they are converted
    // to response-headers, which can be logged.

    try {
      delegateJettyLog.log(request, response);
    } catch (Exception e) {
      log.debug(
          "Logging with Jetty's CustomRequestLogFailed with exception {}, stack is \n{}",
          e,
          e.getStackTrace());
    }
  }
}
