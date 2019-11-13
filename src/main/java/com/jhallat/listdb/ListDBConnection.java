package com.jhallat.listdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ListDBConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ListDBConnection.class);

    private final int port;
    private final String host;
    private ListDBSocket socket;
    private ListDBParser parser = new ListDBParser();

    protected ListDBConnection(String host, int port) {
        this.port = port;
        this.host = host;
    }

    public void open() throws ListDBException {

        socket = new ListDBSocket(host, port);
        socket.open();

    }

    public void close() {
        socket.close();
    }



    public List<Record> getTopics() throws ListDBException {
        List<Record> topics = new ArrayList<>();
        String response = socket.request("list topic");
        topics = parser.parseData(response);
        return topics;
    }

    public Topic getTopic(String id) throws ListDBException {

        //confirm topic exists
        List<Record> topics = getTopics();
        if (!topics.stream().anyMatch(item -> item.getValue().equals(id))) {
            throw new ListDBException("Topic " + id + " does not exists.");
        }
        Topic topic = new Topic(id, new ListDBSocket(host, port));
        topic.open();
        return topic;

    }
}
