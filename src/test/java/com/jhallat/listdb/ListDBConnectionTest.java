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
        List<Record> topics = connection.getTopics();
        connection.close();
        assertNotNull(topics);
        assertTrue(topics.size() > 0);
    }

    @Test
    public void get_topic_should_return_a_new_topic() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource("127.0.0.1", 8888);
        ListDBConnection connection = dataSource.connect();
        Topic topic = connection.getTopic("todo");
        connection.close();
        assertNotNull(topic);
    }

    @Test
    public void list_should_return_a_list_of_items_in_a_topic() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource("127.0.0.1", 8888);
        ListDBConnection connection = dataSource.connect();
        Topic topic = connection.getTopic("todo");
        List<Record> list = topic.list();
        connection.close();
        assertNotNull(list);
        assertTrue(list.size() > 0);
        for (Record item: list) {
            System.out.println(item);
        }
    }
}
