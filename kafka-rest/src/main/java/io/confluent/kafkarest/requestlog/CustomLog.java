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

  private String[] requestAttributesToLog;

  public CustomLog(RequestLog.Writer writer, String formatString, String[] requestAttributesToLog) {
    for (String attr : requestAttributesToLog) {
      formatString += " %{" + attr + "}o";
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

  @Override
  public void log(Request request, Response response) {
    // Add request-attributes to log as response-headers. Then these response-headers are logged
    // in jetty's CustomRequestLog with format specifier "%{AttributeName}o". This is workaround
    // as the Jetty's CustomRequestLog doesn't have ability to log request-attributes. Whereas
    // request-attributes are used to propagate custom-info to log, as that is more idiomatic. Also
    // since request is more readily available Vs response(example - GlobalDosFilterListener), and
    // the attributes are
    for (String attr : this.requestAttributesToLog) {
      Object attrVal = request.getAttribute(attr);
      if (attrVal != null) {
        request.removeAttribute(attr);
        response.setHeader(attr, attrVal.toString());
      }
    }

    try {
      delegateJettyLog.log(request, response);
    } catch (Exception e) {
      log.info(
          "Logging with Jetty's CustomRequestLogFailed with exception {}, stack is \n{}",
          e,
          e.getStackTrace());
    } finally {
      // Remove the response-headers that were added above just for logging.
      for (String attr : this.requestAttributesToLog) {
        response.getHttpFields().remove(attr);
      }
    }
  }
}
