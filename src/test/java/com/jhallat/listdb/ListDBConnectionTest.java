package com.jhallat.listdb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ListDBConnectionTest {

    private Process process;

    @BeforeAll
    public void setup() throws IOException, URISyntaxException {

        InputStream propertyStream = ListDBConnectionTest.class.getResourceAsStream("/listdb-test.properties");
        if (propertyStream == null) {
            fail("Property stream is null");
        }
        Properties properties = new Properties();
        properties.load(propertyStream);
        String dbServer = properties.getProperty("db.server", "");
        int dbPort = Integer.parseInt(properties.getProperty("db.port", "0"));
        String dbInitial = properties.getProperty("db.initial", "");
        String dbActive = properties.getProperty("db.active", "");
        File activeFolder = new File(dbActive);
        File[] contents = activeFolder.listFiles();
        for (int i = contents.length -1; i >= 0; i--) {
            contents[i].delete();
        }
        File initialFolder = new File(dbInitial);
        contents = initialFolder.listFiles();
        for (File file : contents) {
            Path src = Paths.get(file.getPath());
            Path dest = Paths.get(activeFolder.getPath() + "/" + file.getName());
            Files.copy(src, dest);
        }

        process = new ProcessBuilder(dbServer,
                "-p\"server.port=" + dbPort + "\"",
                "-p\"data.home=" + activeFolder.getPath() + "\"").start();
    }

    @AfterAll
    public void tearDown() {
        process.destroy();
    }

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
        Topic topic = connection.getTopic("items");
        connection.close();
        assertNotNull(topic);
    }

    @Test
    public void list_should_return_a_list_of_items_in_a_topic() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource("127.0.0.1", 8888);
        ListDBConnection connection = dataSource.connect();
        Topic topic = connection.getTopic("items");
        List<Record> list = topic.list();
        connection.close();
        assertNotNull(list);
        assertTrue(list.size() == 2);
        Optional<Record> firstActual = list.stream().filter(record -> record.getValue().equals("ItemOne")).findFirst();
        Optional<Record> secondActual = list.stream().filter(record -> record.getValue().equals("ItemTwo")).findFirst();
        assertTrue(firstActual.isPresent());
        assertTrue(secondActual.isPresent());
    }

    @Test
    public void create_topic_should_create_new_topic_and_return() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource("127.0.0.1", 8888);
        ListDBConnection connection = dataSource.connect();
        Topic topic = connection.createTopic("new-for-test");
        List<Record> topics = connection.getTopics();
        Record record = topics.stream().filter(item -> item.getValue().equals("new-for-test")).findFirst().orElse(null);
        connection.close();
        assertNotNull(topic);
        assertNotNull(record);
    }

    @Test
    public void should_delete_topic() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource("127.0.0.1", 8888);
        ListDBConnection connection = dataSource.connect();
        connection.dropTopic("items-to-delete");
        List<Record> topics = connection.getTopics();
        Optional<Record> deleted = topics.stream().filter(record -> record.getValue().equals("items-to-delete")).findFirst();
        assertFalse(deleted.isPresent());
    }
}
