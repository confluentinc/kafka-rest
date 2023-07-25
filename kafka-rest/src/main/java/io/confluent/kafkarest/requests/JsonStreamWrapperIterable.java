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

package io.confluent.kafkarest.requests;

import io.confluent.kafkarest.exceptions.BadRequestException;
import io.confluent.kafkarest.response.JsonStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class JsonStreamWrapperIterable<T> implements Closeable, Iterable<RequestOrError<T>> {
  private final JsonStream<T> jsonStream;

  public JsonStreamWrapperIterable(JsonStream<T> jsonStream) {
    this.jsonStream = Objects.requireNonNull(jsonStream);
  }

  @Override
  public void close() throws IOException {
    jsonStream.close();
  }

  @Override
  public Iterator<RequestOrError<T>> iterator() {
    return new InnerIterator<>(jsonStream);
  }

  public static final class InnerIterator<V> implements Iterator<RequestOrError<V>> {
    private final JsonStream<V> delegate;

    private InnerIterator(JsonStream<V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      try {
        return delegate.hasNext();
      } catch (Throwable re) {
        throw new BadRequestException(
            String.format("Error processing message: %s", re.getMessage()), re);
      }
    }

    @Override
    public RequestOrError<V> next() {
      try {
        return new RequestOrError<>(delegate.nextValue(), null);
      } catch (Throwable throwable) {
        return new RequestOrError<>(null, throwable);
      }
    }
  }
}
