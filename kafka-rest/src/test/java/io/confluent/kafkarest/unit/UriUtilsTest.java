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

package io.confluent.kafkarest.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.UriUtils;
import java.net.URI;
import java.util.Properties;
import javax.ws.rs.core.UriInfo;
import org.apache.kafka.common.config.ConfigException;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UriUtilsTest {

  private UriInfo uriInfo;

  @BeforeEach
  public void setUp() {
    uriInfo = EasyMock.createMock(UriInfo.class);
  }

  @Test
  public void testAbsoluteURIBuilderDefaultHost() {
    KafkaRestConfig config = new KafkaRestConfig();
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com"));
    EasyMock.replay(uriInfo);
    assertEquals("http://foo.com", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }

  @Test
  public void testAbsoluteURIBuilderOverrideHost() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com"));
    EasyMock.replay(uriInfo);
    assertEquals("http://bar.net", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }

  @Test
  public void testAbsoluteURIBuilderWithPort() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    props.put(KafkaRestConfig.PORT_CONFIG, 5000);
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com:5000"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com:5000"));
    EasyMock.replay(uriInfo);
    assertEquals("http://bar.net:5000", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }

  @Test
  public void testAbsoluteURIBuilderWithInvalidListener() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    props.put(KafkaRestConfig.LISTENERS_CONFIG, "http:||0.0.0.0:9091");
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com:9091"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com:9091"));
    EasyMock.replay(uriInfo);

    assertThrows(ConfigException.class, () -> UriUtils.absoluteUri(config, uriInfo));
  }

  @Test
  public void testAbsoluteURIBuilderWithListenerForHttp() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    props.put(KafkaRestConfig.LISTENERS_CONFIG, "http://0.0.0.0:9091,https://0.0.0.0:9092");
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com:9091"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com:9091"));
    EasyMock.replay(uriInfo);
    assertEquals("http://bar.net:9091", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }

  @Test
  public void testAbsoluteURIBuilderWithListenerForHttps() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    props.put(KafkaRestConfig.LISTENERS_CONFIG, "http://0.0.0.0:9091,https://0.0.0.0:9092");
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("https://foo.com:9092"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("https://foo.com:9092"));
    EasyMock.replay(uriInfo);
    assertEquals("https://bar.net:9092", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }

  @Test
  public void testAbsoluteURIBuilderWithIPV6Listener() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    props.put(KafkaRestConfig.LISTENERS_CONFIG, "http://[fe80:0:1:2:3:4:5:6]:9092");
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com:9092"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com:9092"));
    EasyMock.replay(uriInfo);
    assertEquals("http://bar.net:9092", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }

  @Test
  public void testAbsoluteURIBuilderWithTruncatedIPV6Listener() {
    Properties props = new Properties();
    props.put(KafkaRestConfig.HOST_NAME_CONFIG, "bar.net");
    props.put(KafkaRestConfig.LISTENERS_CONFIG, "http://[fe80::1]:9092");
    KafkaRestConfig config = new KafkaRestConfig(props);
    EasyMock.expect(uriInfo.getAbsolutePath()).andStubReturn(URI.create("http://foo.com:9092"));
    EasyMock.expect(uriInfo.getBaseUri()).andReturn(URI.create("http://foo.com:9092"));
    EasyMock.replay(uriInfo);
    assertEquals("http://bar.net:9092", UriUtils.absoluteUri(config, uriInfo));
    EasyMock.verify(uriInfo);
  }
}
