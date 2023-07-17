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

import io.confluent.kafkarest.ratelimit.RateLimitExceededException.ErrorCodes;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.servlets.DoSFilter;
import org.eclipse.jetty.servlets.DoSFilter.Action;
import org.eclipse.jetty.servlets.DoSFilter.OverLimit;

/**
 * This class is a Jetty DosFilter.Listener, for the per-connection-dos(aka per IP, or non-global)
 * filter. This on 429s will populate relevant metadata as attributed on the request, that later-on
 * can be logged.
 */
public class PerConnectionDosFilterListener extends DoSFilter.Listener {

  @Override
  public Action onRequestOverLimit(
      HttpServletRequest request, OverLimit overlimit, DoSFilter dosFilter) {
    // KREST-10418: we don't use super function to get action object because
    // it will log a WARN line, in order to reduce verbosity
    Action action = Action.fromDelay(dosFilter.getDelayMs());
    if (action.equals(Action.REJECT)) {
      request.setAttribute(
          CustomLogRequestAttributes.REST_ERROR_CODE,
          ErrorCodes.DOS_FILTER_MAX_REQUEST_PER_CONNECTION_LIMIT_EXCEEDED);
    }
    return action;
  }
}
