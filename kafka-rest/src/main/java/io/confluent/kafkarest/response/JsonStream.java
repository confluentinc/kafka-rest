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
import io.confluent.kafkarest.exceptions.ProduceRequestTooLargeException;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A lazy wrapper over {@link MappingIterator} to delay {@link
 * com.fasterxml.jackson.core.JsonParser} until the first read.
 */
public final class JsonStream<T> implements Closeable {
  private final Supplier<MappingIterator<T>> delegate;
  private final SizeLimitEntityStream inputStream;

  public JsonStream(Supplier<MappingIterator<T>> delegate) {
    this(delegate, null);
  }

  public JsonStream(Supplier<MappingIterator<T>> delegate, SizeLimitEntityStream inputStream) {
    this.delegate = Suppliers.memoize(delegate::get);
    this.inputStream = inputStream;
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
    T value = delegate.get().nextValue();
    if (inputStream != null) {
      inputStream.resetCounter();
    }
    return value;
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.invalidateCounter();
    }
    if (delegate.get() != null) {
      delegate.get().close();
    }
  }

  /**
   * This class provides an {@link InputStream} where we can keep track of how many bytes have been
   * read while parsing a produce request so that we can act accordingly if the input stream is too
   * large; as an input stream is unbounded, we provide a method to reset the byte counter so that
   * it will be called by {@link JsonStream} on the boundary whenever a produce request is
   * successfully parsed; note that this class might not provide 100% accuracy produce request size
   * counting due to the default buffer size of Jackson parser is 8000, so it is recommended to use
   * this class only to validate produce request of big size.
   */
  public static class SizeLimitEntityStream extends InputStream {

    private final InputStream delegate;
    private final long sizeThreshold;
    // this keeps track of how many bytes have been read while parsing a produce request
    private final AtomicLong produceRequestByteCounter = new AtomicLong(0);

    public SizeLimitEntityStream(InputStream delegate, long sizeThreshold) {
      this.delegate = delegate;
      this.sizeThreshold = sizeThreshold;
    }

    @Override
    public int read() throws IOException {
      int v = delegate.read();
      if (v != -1) {
        validateSize(1);
      }
      return v;
    }

    @Override
    public int read(byte[] b) throws IOException {
      int v = delegate.read(b);
      if (v != -1) {
        validateSize(v);
      }
      return v;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int v = delegate.read(b, off, len);
      if (v != -1) {
        validateSize(v);
      }
      return v;
    }

    @Override
    public long skip(long n) throws IOException {
      long v = delegate.skip(n);
      validateSize(v);
      return v;
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
      delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      delegate.reset();
    }

    @Override
    public boolean markSupported() {
      return delegate.markSupported();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    /** This method must be called every time a ProduceRequest is successfully parsed */
    private void resetCounter() {
      produceRequestByteCounter.set(0);
    }

    /**
     * This method must be called when {@link JsonStream} is closed and must be called only once.
     * The reason is that when JsonStream is closed, it would still attempt to parse the InputStream
     * if the delegate MappingIterator isn't cached in Suppliers.memoize. We set the counter to
     * Long.MIN_VALUE to make sure validateSize never throws.
     */
    private void invalidateCounter() {
      produceRequestByteCounter.set(Long.MIN_VALUE);
    }

    private void validateSize(long add) {
      if (produceRequestByteCounter.addAndGet(add) > sizeThreshold) {
        throw new ProduceRequestTooLargeException();
      }
    }
  }
}
