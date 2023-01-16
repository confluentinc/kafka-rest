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

package io.confluent.kafkarest.converters;

import javax.validation.ConstraintViolationException;

/**
 * Exception thrown when conversion fails. Since this should generally be converted into a 422 HTTP
 * status, this class extends ConstraintViolationException so you get the expected behavior if you
 * don't catch the exception.
 */
public class ConversionException extends ConstraintViolationException {

  ConversionException(String msg) {
    super(msg, null);
  }
}
