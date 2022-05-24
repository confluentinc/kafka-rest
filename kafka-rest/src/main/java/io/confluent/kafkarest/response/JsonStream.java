/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.response;

import com.fasterxml.jackson.databind.MappingIterator;
import com.google.common.base.Suppliers;
import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A lazy wrapper over {@link MappingIterator} to delay {@link
 * com.fasterxml.jackson.core.JsonParser} until the first read.
 */
public final class JsonStream<T> implements Closeable {
  private final Supplier<MappingIterator<T>> delegate;

  public JsonStream(Supplier<MappingIterator<T>> delegate) {
    this.delegate = Suppliers.memoize(delegate::get);
  }

  public boolean hasNext() {
    if (delegate.get() == null) {
      return false;
    }
    return delegate.get().hasNext();
  }

  public T nextValue() throws IOException {
    if (delegate.get() == null) {
      throw new NoSuchElementException();
    }
    return delegate.get().nextValue();
  }

  @Override
  public void close() throws IOException {
    if (delegate.get() != null) {
      delegate.get().close();
    }
  }
}
