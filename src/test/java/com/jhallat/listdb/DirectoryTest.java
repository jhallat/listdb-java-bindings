package com.jhallat.listdb;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DirectoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryTest.class);
    private static final String DB_HOST = "127.0.0.1";
    private static final int DB_PORT = 8888;

    private Process process;

    @BeforeAll
    public void setup() throws IOException, URISyntaxException, InterruptedException {

        InputStream propertyStream = DirectoryTest.class.getResourceAsStream("/listdb-test.properties");
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
            if (contents[i].isDirectory()) {
                FileUtils.deleteDirectory(contents[i]);
            } else {
                contents[i].delete();
            }
        }
        File initialFolder = new File(dbInitial);
        contents = initialFolder.listFiles();
        for (File file : contents) {
            LOG.info("{} is directory {}", file.getPath(), file.isDirectory());
            Path src = Paths.get(file.getPath());
            Path dest = Paths.get(activeFolder.getPath() + "/" + file.getName());
            Files.copy(src, dest);
            if (file.isDirectory()) {
                File destFolder = new File(activeFolder.getPath() + "/" + file.getName());
                LOG.info("copy folder {} to {}", file.getPath(), destFolder.getPath());
                FileUtils.copyDirectory(file, destFolder);
            }

        }

        //ProcessBuilder processBuilder = new ProcessBuilder(dbServer,
        //        "-p\"server.port=" + dbPort + "\"",
        //        "-p\"data.home=" + activeFolder.getPath() + "\"");

        //process = processBuilder.start();
        String command = String.format("%s -p\"server.port=%s\" -p\"data.home=%s\"",
               dbServer,
                dbPort,
                activeFolder.getPath());
        LOG.info(command);
        //process = Runtime.getRuntime().exec(command);
        //Map<String, String> environment = processBuilder.environment();
        //environment.put("RUST_LOG","listdb_server=debug");
        //processBuilder.redirectOutput(new File("test-server.log"));
        //process = processBuilder.start();
        //BufferedReader output = new BufferedReader(new InputStreamReader(process.getInputStream()));
        //System.out.println(output.readLine());

    }

    @AfterAll
    public void tearDown() {
        //process.destroy();
    }

    @Test
    public void open_connection_should_return_topics() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory rootDirectory = dataSource.getRootDirectory();
        List<Record> topics = rootDirectory.getTopics();
        rootDirectory.close();
        assertNotNull(topics);
        assertTrue(topics.size() > 0);
    }

    @Test
    public void initialize_with_directory_other_than_root() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory directory = dataSource.getDirectoryFromPath("directory-one");
        List<Record> topics = directory.getTopics();
        directory.close();
        assertNotNull(topics);
        Optional<Record> firstActual = topics.stream().filter(record -> record.getValue().equals("child-topic")).findFirst();
        assertTrue(firstActual.isPresent());
    }

    @Test
    public void open_connection_should_return_directories() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory connection = dataSource.getRootDirectory();
        List<Record> directories = connection.getDirectories();
        connection.close();
        assertNotNull(directories);
        assertTrue(directories.size() > 0);
    }

    @Test
    public void get_topic_should_return_a_new_topic() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory connection = dataSource.getRootDirectory();
        Topic topic = connection.getTopic("items");
        connection.close();
        assertNotNull(topic);
    }

    @Test
    public void get_directory_should_return_a_new_directory() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory rootDirectory = dataSource.getRootDirectory();
        Directory childDirectory = rootDirectory.getDirectory("directory-one");
        assertNotNull(childDirectory);
        List<Record> topics = childDirectory.getTopics();
        Optional<Record> firstActual = topics.stream().filter(record -> record.getValue().equals("child-topic")).findFirst();
        assertTrue(firstActual.isPresent());
    }

    @Test
    public void list_should_return_a_list_of_items_in_a_topic() throws ListDBException {
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory connection = dataSource.getRootDirectory();
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
        ListDBDataSource dataSource = new ListDBDataSource(DB_HOST, DB_PORT);
        Directory connection = dataSource.getRootDirectory();
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
        Directory connection = dataSource.getRootDirectory();
        connection.dropTopic("items-to-delete");
        List<Record> topics = connection.getTopics();
        Optional<Record> deleted = topics.stream().filter(record -> record.getValue().equals("items-to-delete")).findFirst();
        assertFalse(deleted.isPresent());
    }
}
