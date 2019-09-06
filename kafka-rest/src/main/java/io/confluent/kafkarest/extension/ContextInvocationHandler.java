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

package io.confluent.kafkarest.extension;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import io.confluent.kafkarest.KafkaRestContext;


public class ContextInvocationHandler implements InvocationHandler {

  public ContextInvocationHandler() {
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    KafkaRestContext context = KafkaRestContextProvider.getCurrentContext();
    return method.invoke(context, args);
  }
}
