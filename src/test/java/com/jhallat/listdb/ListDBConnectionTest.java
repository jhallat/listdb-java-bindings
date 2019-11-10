package com.jhallat.listdb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListDBConnectionTest {

    @Test
    public void open_connection_should_return_topics() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource("127.0.0.1", 8888);
        ListDBConnection connection = dataSource.connect();
        List<String> topics = connection.getTopics();
        assertNotNull(topics);
        assertTrue(topics.size() > 0);
    }

}
