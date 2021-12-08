package io.confluent.kafkarest.response;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.MappingIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FakeTimeLimiter;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.errors.AuthenticationException;
import org.glassfish.jersey.server.ChunkedOutput;
import org.glassfish.jersey.server.internal.LocalizationMessages;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingResponseTest {

  private FakeChunkedOutputFactory chunkedOutputFactory;
  private FakeAsyncResponse response;

  @Before
  public void setUp() {
    chunkedOutputFactory = new FakeChunkedOutputFactory();
    response = new FakeAsyncResponse();
  }

  @Test
  public void testGracePeriodExceededExceptionThrown() throws IOException {
    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            chunkedOutputFactory,
            new StreamingResponseIdleTimeLimiter(new FakeTimeLimiter(), Duration.ZERO));

    MappingIterator<Integer> input = mock(MappingIterator.class);
    expect(input.hasNextValue())
        .andReturn(true)
        .andReturn(true)
        .andReturn(true)
        .andStubReturn(false);
    expect(input.nextValue())
        .andReturn(1)
        .andReturn(2)
        .andReturn(3)
        .andStubThrow(new NoSuchElementException());
    replay(input);

    streamingResponseFactory
        .from(input)
        .compose(result -> CompletableFuture.completedFuture(10 + result))
        .resume(response);

    assertEquals(
        ImmutableList.of(
            ResultOrError.result(11), ResultOrError.result(12), ResultOrError.result(13)),
        chunkedOutputFactory.getChunkedOutput().getWritten());
  }

  @Test
  public void testWriteToChunkedOutput() throws IOException {
    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            chunkedOutputFactory,
            new StreamingResponseIdleTimeLimiter(new FakeTimeLimiter(), Duration.ZERO));

    MappingIterator<Integer> input = mock(MappingIterator.class);
    expect(input.hasNextValue())
        .andReturn(true)
        .andReturn(true)
        .andReturn(true)
        .andStubReturn(false);
    expect(input.nextValue())
        .andReturn(1)
        .andReturn(2)
        .andReturn(3)
        .andStubThrow(new NoSuchElementException());
    replay(input);

    streamingResponseFactory
        .from(input)
        .compose(result -> CompletableFuture.completedFuture(10 + result))
        .resume(response);

    assertEquals(
        ImmutableList.of(
            ResultOrError.result(11), ResultOrError.result(12), ResultOrError.result(13)),
        chunkedOutputFactory.getChunkedOutput().getWritten());
  }

  @Test
  public void testStreamIsClosedIfIdleTimeExceeded() throws IOException {
    ExecutorService idleTimeExecutor = Executors.newSingleThreadExecutor();
    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            chunkedOutputFactory,
            new StreamingResponseIdleTimeLimiter(
                SimpleTimeLimiter.create(idleTimeExecutor), Duration.ofMillis(100)));

    MappingIterator<Integer> input = mock(MappingIterator.class);
    expect(input.hasNextValue())
        .andReturn(true)
        .andAnswer(
            () -> {
              Thread.sleep(200);
              return true;
            })
        .andReturn(true)
        .andStubReturn(false);
    expect(input.nextValue())
        .andReturn(1)
        .andReturn(2)
        .andReturn(3)
        .andStubThrow(new NoSuchElementException());
    replay(input);

    streamingResponseFactory
        .from(input)
        .compose(result -> CompletableFuture.completedFuture(10 + result))
        .resume(response);

    assertEquals(
        ImmutableList.of(
            ResultOrError.result(11),
            ResultOrError.error(
                ErrorResponse.create(408, "Timeout while reading from inputStream"))),
        chunkedOutputFactory.getChunkedOutput().getWritten());
  }

  @Test
  public void testStreamIsClosedIfUnauthenticated() throws IOException {
    StreamingResponseFactory streamingResponseFactory =
        new StreamingResponseFactory(
            chunkedOutputFactory,
            new StreamingResponseIdleTimeLimiter(new FakeTimeLimiter(), Duration.ZERO));

    MappingIterator<Integer> input = mock(MappingIterator.class);
    expect(input.hasNextValue())
        .andReturn(true)
        .andReturn(true)
        .andReturn(true)
        .andStubReturn(false);
    expect(input.nextValue())
        .andReturn(1)
        .andReturn(2)
        .andReturn(3)
        .andStubThrow(new NoSuchElementException());
    replay(input);

    streamingResponseFactory
        .from(input)
        .compose(result -> CompletableFutures.failedFuture(new AuthenticationException("foobar")))
        .resume(response);

    assertEquals(
        ImmutableList.of(ResultOrError.error(ErrorResponse.create(40101, "foobar"))),
        chunkedOutputFactory.getChunkedOutput().getWritten());
  }

  private static final class FakeChunkedOutputFactory extends ChunkedOutputFactory {
    private final FakeChunkedOutput<ResultOrError> chunkedOutput =
        new FakeChunkedOutput<>(ResultOrError.class);

    @Override
    public FakeChunkedOutput<ResultOrError> getChunkedOutput() {
      return chunkedOutput;
    }
  }

  private static final class FakeChunkedOutput<T> extends ChunkedOutput<T> {
    private final ArrayList<T> written = new ArrayList<>();

    private boolean isClosed = false;

    private FakeChunkedOutput(Class<? extends T> chunkType) {
      super(chunkType);
    }

    @Override
    public void write(T chunk) throws IOException {
      if (isClosed) {
        throw new IOException(LocalizationMessages.CHUNKED_OUTPUT_CLOSED());
      }
      written.add(chunk);
    }

    @Override
    protected void flushQueue() {}

    @Override
    public void close() {
      isClosed = true;
    }

    @Override
    public boolean isClosed() {
      return isClosed;
    }

    @Override
    protected void onClose(Exception e) {}

    private ImmutableList<T> getWritten() {
      return ImmutableList.copyOf(written);
    }
  }
}
