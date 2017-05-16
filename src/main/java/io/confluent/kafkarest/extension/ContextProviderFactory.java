/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.extension;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import org.glassfish.hk2.api.Factory;

import io.confluent.kafkarest.Context;

public class ContextProviderFactory implements Factory<Context> {

  private final ContainerRequestContext requestContext;

  @Inject
  public ContextProviderFactory(ContainerRequestContext context) {
    this.requestContext = context;
  }

  @Override
  public Context provide() {
    if (requestContext.getProperty("rest-proxy-context") != null) {
      return (Context) requestContext.getProperty("rest-proxy-context");
    } else {
      return DefaultContextProvider.getDefaultContext();
    }
  }

  @Override
  public void dispose(Context t) {
  }
}
