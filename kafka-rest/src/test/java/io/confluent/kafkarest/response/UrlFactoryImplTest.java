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
import static org.junit.Assert.assertEquals;

import java.net.URI;
import javax.ws.rs.core.UriInfo;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UrlFactoryImplTest {

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private UriInfo requestUriInfo;

  @Test
  public void create_withHostNameAndPortConfig_returnsUrlRelativeToHostNameAndPortConfig() {
    expect(requestUriInfo.getAbsolutePath()).andReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
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
            singletonList("http://advertised.listener:2000"),
            singletonList("http://listener:3000"),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://advertised.listener:2000/foo/bar", url);
  }

  @Test
  public void
  create_withAdvertisedListenerDifferentSchemeAndListenerSameScheme_returnsUrlRelativeToListenerSameScheme
      () {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            singletonList("https://advertised.listener:2000"),
            singletonList("http://listener:3000"),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://listener:3000/foo/bar", url);
  }

  @Test
  public void create_withListenerSameScheme_returnsUrlRelativeToListenerSameScheme() {
    expect(requestUriInfo.getAbsolutePath())
        .andStubReturn(URI.create("http://1.2.3.4:1000/xxx/yyy"));
    expect(requestUriInfo.getBaseUri()).andReturn(URI.create("http://1.2.3.4:1000/"));
    replay(requestUriInfo);

    UrlFactory urlFactory =
        new UrlFactoryImpl(
            /* hostNameConfig= */ "",
            /* portConfig= */ 0,
            /* advertisedListeners= */ emptyList(),
            singletonList("http://listener:2000"),
            requestUriInfo);

    String url = urlFactory.create("foo", "bar");

    assertEquals("http://listener:2000/foo/bar", url);
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
            singletonList("https://listener:2000"),
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
}
