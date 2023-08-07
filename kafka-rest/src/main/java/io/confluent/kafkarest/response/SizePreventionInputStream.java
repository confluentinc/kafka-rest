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

package io.confluent.kafkarest.response;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizePreventionInputStream extends InputStream {
  private static final Logger log = LoggerFactory.getLogger(SizePreventionInputStream.class);

  private static final long SIZE_THRESHOLD = 27L * 1024 * 1024;

  private final InputStream delegate;
  private final AtomicLong bytesRead = new AtomicLong(0);

  public SizePreventionInputStream(InputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public int read() throws IOException {
    int v = delegate.read();
    log.debug("read(): {}, total: {}", v, bytesRead.get());
    if (v != -1) {
      checkSize(1);
    }
    return v;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public int read(@NotNull byte[] b) throws IOException {
    int v = delegate.read(b);
    log.debug("read(b): {}, total: {}", v, bytesRead.get());
    if (v != -1) {
      checkSize(v);
    }
    return v;
  }

  @Override
  public int read(@NotNull byte[] b, int off, int len) throws IOException {
    int v = delegate.read(b, off, len);
    log.debug("read(b,off,len): {}, total: {}", v, bytesRead.get());
    if (v != -1) {
      checkSize(v);
    }
    return v;
  }

  @Override
  public long skip(long n) throws IOException {
    long v = delegate.skip(n);
    log.debug("skip(): {}, total: {}", v, bytesRead.get());
    checkSize(v);
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

  public void resetBytesRead() {
    bytesRead.set(0);
  }

  private void checkSize(long add) throws IOException {
    if (bytesRead.addAndGet(add) >= SIZE_THRESHOLD) {
      throw new IllegalArgumentException("Input stream too large");
    }
  }
}
