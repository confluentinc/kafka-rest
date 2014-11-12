package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.junit.ResourceTestRule;
import io.confluent.kafkarest.resources.TopicsResource;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TopicsResourceTest {
    private static Config config = new Config();
    private static MetadataObserver mdObserver = EasyMock.createMock(MetadataObserver.class);
    private static Context ctx = new Context(config, mdObserver);

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new TopicsResource(ctx))
            .build();

    @Before
    public void setUp() {
        EasyMock.reset(mdObserver);
    }

    @Test
    public void testList() {
        final List<Topic> topics = Arrays.asList(
                new Topic("test1", 10),
                new Topic("test2", 1),
                new Topic("test3", 10)
        );
        EasyMock.expect(mdObserver.getTopics()).andReturn(topics);
        EasyMock.replay(mdObserver);

        final List<Topic> response = resources.client().target("/topics")
                .request().get(new GenericType<List<Topic>>(){});
        assertEquals(topics, response);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetTopic() {
        Topic topic1 = new Topic("topic1", 2);
        Topic topic2 = new Topic("topic2", 5);
        EasyMock.expect(mdObserver.getTopic("topic1"))
                .andReturn(topic1);
        EasyMock.expect(mdObserver.getTopic("topic2"))
                .andReturn(topic2);
        EasyMock.replay(mdObserver);

        final Topic response1 = resources.client().target("/topics/topic1")
                .request().get(new GenericType<Topic>(){});
        assertEquals(topic1, response1);

        final Topic response2 = resources.client().target("/topics/topic2")
                .request().get(new GenericType<Topic>(){});
        assertEquals(topic2, response2);

        EasyMock.verify(mdObserver);
    }

    @Test
    public void testGetInvalidTopic() {
        EasyMock.expect(mdObserver.getTopic("nonexistanttopic"))
                .andReturn(null);
        EasyMock.replay(mdObserver);

        Response response = resources.client().target("/topics/nonexistanttopic")
                .request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

        EasyMock.verify(mdObserver);
    }
}
