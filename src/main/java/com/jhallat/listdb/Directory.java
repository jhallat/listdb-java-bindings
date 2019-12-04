package com.jhallat.listdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class Directory {

    private static final Logger LOG = LoggerFactory.getLogger(Directory.class);

    private final int port;
    private final String host;
    private final String contextPath;
    private ListDBSocket socket;
    private ListDBParser parser = new ListDBParser();

    protected Directory(String host, int port) {
        this(host, port, "");
    }

    protected Directory(String host, int port, String contextPath) {
        this.port = port;
        this.host = host;
        this.contextPath = contextPath;
    }

    public void open() throws ListDBException {

        socket = new ListDBSocket(host, port);
        socket.open();
        if (!contextPath.isEmpty()) {
            ListDBResponse response = socket.request("open directory " + contextPath);
            LOG.debug("open connection response = {}", response);
        }

    }

    public void close() {
        socket.close();
    }


    public List<Record> getTopics() throws ListDBException {
        List<Record> topics = new ArrayList<>();
        ListDBResponse response = socket.request("list topic");
        if (response.getResponseType() == ListDBResponseType.DATA) {
            topics = parser.parseData(response.getContents());
        } else {
            parser.handleError(response);
        }
        return topics;
    }

    public List<Record> getDirectories() throws ListDBException {
        List<Record> directories = new ArrayList<>();
        ListDBResponse response = socket.request("list directory");
        if (response.getResponseType() == ListDBResponseType.DATA) {
            directories = parser.parseData(response.getContents());
        } else {
            parser.handleError(response);
        }
        return directories;
    }

    public Topic getTopic(String id) throws ListDBException {

        //confirm topic exists
        List<Record> topics = getTopics();
        if (topics.stream().noneMatch(item -> item.getValue().equals(id))) {
            throw new ListDBException("Topic " + id + " does not exist.");
        }
        String topicId;
        if (!contextPath.isEmpty()) {
            topicId = contextPath + "/" + id;
        } else {
            topicId = id;
        }
        Topic topic = new Topic(topicId, new ListDBSocket(host, port));
        topic.open();
        return topic;

    }

    public Directory getDirectory(String id) throws ListDBException {
        List<Record> directories = getDirectories();
        if (directories.stream().noneMatch(item -> item.getValue().equals(id))) {
            throw new ListDBException("Directory " + id + " does not exist.");
        }
        Directory directory = new Directory(host, port, id);
        directory.open();
        return directory;
    }


    public Topic createTopic(String id) throws ListDBException {
        ListDBResponse response = socket.request("create topic " + id);
        if (response.getResponseType() == ListDBResponseType.OK) {
            return getTopic(id);
        } else {
            parser.handleError(response);
        }
        throw new ListDBException("UNKNOWN");
    }

    public void dropTopic(String id) throws ListDBException {
        ListDBResponse response = socket.request("drop topic " + id);
        if (response.getResponseType() != ListDBResponseType.OK) {
            parser.handleError(response);
        }
    }
}
