/*
 * Copyright 2020 Confluent Inc.
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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import javax.ws.rs.core.UriInfo;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class UrlFactoryImplTest {

  @Mock private UriInfo requestUriInfo;

  @Test
  public void create_withHostNameAndPortConfig_returnsUrlRelativeToHostNameAndPortConfig() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl("hostname", 2000, emptyList(), emptyList(), requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://hostname:2000/foo/bar", url);
  }

  @Test
  public void
      create_withAdvertisedListenerSameScheme_returnsUrlRelativeToAdvertisedListenerSameScheme() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            singletonList(URI.create("http://advertised.listener:2000")),
            singletonList(URI.create("http://listener:3000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://advertised.listener:2000/foo/bar", url);
  }

  @Test
  public void
      create_withAdvertisedListenerDifferentSchemeAndListenerSameScheme_returnsUrlRelativeToRequestUri() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            singletonList(URI.create("https://advertised.listener:2000")),
            singletonList(URI.create("http://listener:3000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://1.2.3.4:1000/foo/bar", url);
  }

  @Test
  public void create_withListenerSameScheme_returnsUrlRelativeToRequestUri() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            /* advertisedListeners= */ emptyList(),
            singletonList(URI.create("http://listener:2000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://1.2.3.4:1000/foo/bar", url);
  }

  @Test
  public void create_withListenerDifferentScheme_returnsUrlRelativeToRequestUri() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            /* advertisedListeners= */ emptyList(),
            singletonList(URI.create("https://listener:2000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://1.2.3.4:1000/foo/bar", url);
  }

  @Test
  public void create_withoutListeners_returnsUrlRelativeToRequestUri() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            /* advertisedListeners= */ emptyList(),
            /* listeners= */ emptyList(),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://1.2.3.4:1000/foo/bar", url);
  }

  @Test
  public void create_withoutListenersAndWithBasePath_returnsUrlRelativeToRequestUriAndBasePath() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/xxx/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            /* advertisedListeners= */ emptyList(),
            /* listeners= */ emptyList(),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://1.2.3.4:1000/xxx/foo/bar", url);
  }

  @Test
  public void testCreateHostAndAdvertisedListenerReturnsRelativeToAdvertisedListener() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            "hostname",
            2000,
            singletonList(URI.create("http://advertised.listener:2000")),
            emptyList(),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://advertised.listener:2000/foo/bar", url);
  }

  @Test
  public void testCreateHostAndListenerReturnsRelativeToListener() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            "hostname",
            2000,
            emptyList(),
            singletonList(URI.create("http://listener:2000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://hostname:2000/foo/bar", url);
  }

  @Test
  public void testCreateAdvertisedListenerAndListenerReturnsRelativeToAdvertisedListener() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            "",
            0,
            singletonList(URI.create("http://advertised.listener:2000")),
            singletonList(URI.create("http://listener:2000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://advertised.listener:2000/foo/bar", url);
  }

  @Test
  public void testCreateHostAdvertisedListenerAndListenerReturnsRelativeToAdvertisedListener() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            "hostname",
            2000,
            singletonList(URI.create("http://advertised.listener:2000")),
            singletonList(URI.create("http://listener:2000")),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://advertised.listener:2000/foo/bar", url);
  }

  @Test
  public void urlBuilder_urlEncodesQueryParamValues() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl("hostname", 2000, emptyList(), emptyList(), requestUriInfo);
    UrlBuilder urlBuilder = urlFactory.newUrlBuilder();

    String url =
        urlBuilder
            .appendPathSegment("foobar")
            .putQueryParameter("foo", "b a r")
            .putQueryParameter("foz", "b!a@z")
            .build();

    assertEquals("http://hostname:2000/foobar?foo=b+a+r&foz=b%21a%40z", url);
  }
}
