package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.Versions;

import java.util.Properties;
import javax.ws.rs.core.Response;

import org.junit.Test;

import static io.confluent.kafkarest.KafkaRestConfig.REFLECT_XHEADERS_CONFIG;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertFalse;

/**
 * Verifies that the XHeaderReflectingResponseFilter is engaged when 'enable.reflect.xheaders'
 * property is set to 'true'.
 * <p>
 * This is non-exhaustive - testing only a sample set of API calls.
 */
public class XHeadersAreReflectedDisabledTest extends ClusterTestHarness {

  public XHeadersAreReflectedDisabledTest() {
    super(1, false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(REFLECT_XHEADERS_CONFIG, "false");
  }

  @Test
  public void requestXHeaderIsNotReflectedInResponseWhenNotEnabled() {

    // given x-header reflection is disabled and...
    final String xHeaderKey = "x-some-arbitrary-header";
    final String xHeaderValue = "some-value";

    // when
    Response response = request("/topics").header(xHeaderKey, xHeaderValue).get();
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);

    // then
    assertFalse(response.getHeaders().containsKey(xHeaderKey));
  }
}
