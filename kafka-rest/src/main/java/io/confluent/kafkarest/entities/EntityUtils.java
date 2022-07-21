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

package io.confluent.kafkarest.entities;

import javax.xml.bind.DatatypeConverter;

public class EntityUtils {

  public static byte[] parseBase64Binary(String data) throws IllegalArgumentException {
    try {
      return DatatypeConverter.parseBase64Binary(data);
    } catch (ArrayIndexOutOfBoundsException e) {
      // Implementation can throw index error on invalid inputs, make sure all known parsing issues
      // get converted to illegal argument error
      throw new IllegalArgumentException(e);
    }
  }

  public static String encodeBase64Binary(byte[] data) {
    return DatatypeConverter.printBase64Binary(data);
  }
}
