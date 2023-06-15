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

/**
 * This class lists the fields that are added to the request-log(see RestCustomRequestLog.java).
 * These field(& corresponding) value are stored as request-attribute.
 */
public class CustomLogFields {
  public static final String REST_ERROR_CODE_FIELD = "REST_ERROR_CODE";
}
