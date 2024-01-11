/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.extension;

import javax.ws.rs.Priorities;

/**
 * A collection of priority constants to be used for components ordered based on their {@link
 * javax.annotation.Priority} class-level annotation value. "More" because they can be perceived as
 * an add-on to {@link Priorities}.
 */
public final class MorePriorities {

  /**
   * A priority for filters/interceptors that need to be triggered <em>before</em> components with
   * {@link Priorities#AUTHENTICATION}.
   */
  public static final int PRE_AUTHENTICATION = Priorities.AUTHENTICATION - 500; /* lower value ==
                                                                                  higher priority */

  private MorePriorities() {}
}
