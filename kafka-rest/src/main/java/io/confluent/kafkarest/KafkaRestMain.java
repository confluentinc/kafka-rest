/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest;

import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRestMain {

  private static final Logger log = LoggerFactory.getLogger(KafkaRestMain.class);

  static {
    // for localhost testing only, with schema registry url https://localhost
    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
        new javax.net.ssl.HostnameVerifier() {

          public boolean verify(String hostname, javax.net.ssl.SSLSession sslSession) {
            if (hostname.equals("localhost")) {
              return true;
            }
            return false;
          }
        });
  }

  /** Starts an embedded Jetty server running the REST server. */
  public static void main(String[] args) throws IOException {
    try {
      KafkaRestConfig config = new KafkaRestConfig((args.length > 0 ? args[0] : null));

      Slf4jRequestLogWriter logWriter = new Slf4jRequestLogWriter();
      logWriter.setLoggerName(config.getString(RestConfig.REQUEST_LOGGER_NAME_CONFIG));
      RestCustomRequestLog requestLog =
          new RestCustomRequestLog(logWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT + " %{ms}T");
      requestLog.setRequestAttributesToLog(new String[] {CustomLogFields.REST_ERROR_CODE_FIELD});
      KafkaRestApplication app = new KafkaRestApplication(config, "", null, requestLog);
      app.setNonGlobalDosfilterListeners(
          new ArrayList(Arrays.asList(new PerConnectionDosFilterListener())));
      app.setGlobalDosfilterListeners(new ArrayList(Arrays.asList(new GlobalDosFilterListener())));
      app.start();
      log.info("Server started, listening for requests...");
      app.join();
    } catch (RestConfigException e) {
      log.error("Server configuration failed: ", e);
      System.exit(1);
    } catch (Exception e) {
      log.error("Server died unexpectedly: ", e);
      System.exit(1);
    }
  }
}
