package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.Versions;

import java.util.Properties;
import javax.ws.rs.core.Response;

import org.junit.Test;

import static io.confluent.kafkarest.KafkaRestConfig.REFLECT_XHEADERS;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Verifies that the XHeaderReflectingResponseFilter is engaged when 'enable.reflect.xheaders'
 * property is set to 'true'.
 * <p>
 * This is non-exhaustive - testing only a sample set of API calls.
 */
public class XHeadersAreReflectedEnabledTest extends ClusterTestHarness {

  public XHeadersAreReflectedEnabledTest() {
    super(1, false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(REFLECT_XHEADERS, false);
  }


  @Test
  public void requestXHeaderIsReflectedInResponseWhenEnabled() {

    // given x-header reflection is enabled and...
    final String xHeaderKey = "x-some-arbitrary-header";
    final String xHeaderValue = "some-value";

    // when
    Response response = request("/topics").header(xHeaderKey, xHeaderValue).get();
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);

    // then
    assertEquals(xHeaderValue, response.getHeaders().getFirst(xHeaderKey));
  }

  @Test
  public void requestNonXHeaderIsNotReflectedInResponseWhenEnabled() {

    // given x-header reflection is enabled and...
    final String nonXHeaderKey = "some-arbitrary-header";
    final String nonXHeaderValue = "some-value";

    // when
    Response response = request("/topics").header(nonXHeaderKey, nonXHeaderValue).get();
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);

    // then
    assertFalse(response.getHeaders().containsKey(nonXHeaderKey));
  }
}
