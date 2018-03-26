package io.confluent.kafkarest.integration;

import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Versions;
import org.junit.Test;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

/**
 * Verifies that the XHeaderReflectingResponseFilter is engaged when 'enable.reflect.xheaders'
 * property is set to 'true'.
 * <p>
 * This is non-exhaustive - testing only a sample set of API calls.
 */
public class XHeadersAreReflectedTest extends ClusterTestHarness {

  public XHeadersAreReflectedTest() {
    super(1,false);
  }

  @Test
  public void requestXHeaderIsReflectedInResponse() {
    // given the x-header
    final String xHeaderKey = "x-some-arbitrary-header";
    final String xHeaderValue = "some-value";

    // when
    Response response = request("/topics").header(xHeaderKey, xHeaderValue).get();
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);

    // then
    assertEquals(xHeaderValue, response.getHeaders().getFirst(xHeaderKey));
  }
}
