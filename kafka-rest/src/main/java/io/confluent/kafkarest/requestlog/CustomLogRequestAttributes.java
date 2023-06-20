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

/**
 * This class lists the request-attributes that are used to propagate custom-info that will be added
 * to the request-log(see CustomLog.java).
 */
public final class CustomLogRequestAttributes {

  private CustomLogRequestAttributes() {}

  public static final String REST_ERROR_CODE = "REST_ERROR_CODE";
}
