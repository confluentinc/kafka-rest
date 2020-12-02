package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public final class ForwardHeaderTest {

    public static final String KEYS_SINGLE = "[{\"key1\":\"value1\"}]";
    public static final String KEYS_TWO = "[{\"key1\":\"value1\"},{\"key1\":\"value2\"}]";
    ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
    }

    @Test
    public void serialize() throws JsonProcessingException {
        List<ForwardHeader> headers = new ArrayList<>();
        assertEquals("[]", mapper.writeValueAsString(headers));
        headers.add(new ForwardHeader("key1", "value1"));
        assertEquals(KEYS_SINGLE, mapper.writeValueAsString(headers));
        headers.add(new ForwardHeader("key1", "value2"));
        assertEquals(KEYS_TWO, mapper.writeValueAsString(headers));
    }

    @Test
    public void deserialize() throws JsonProcessingException {
        ForwardHeader[] headers = mapper.readValue(KEYS_SINGLE, ForwardHeader[].class);
        assertEquals(1, headers.length);
        assertEquals("key1", headers[0].key);
        assertArrayEquals("value1".getBytes(), headers[0].value);
        headers = mapper.readValue(KEYS_TWO, ForwardHeader[].class);
        assertEquals(2, headers.length);
        assertEquals("key1", headers[0].key);
        assertArrayEquals("value1".getBytes(), headers[0].value);
        assertEquals("key1", headers[1].key);
        assertArrayEquals("value2".getBytes(), headers[1].value);
    }
}