package com.jhallat.listdb;

import java.util.List;

public class Topic {

    private final ListDBSocket socket;
    private final String id;
    private final ListDBParser parser = new ListDBParser();

    protected Topic(String id, ListDBSocket socket) {
        this.socket = socket;
        this.id = id;
    }

    protected void open() throws ListDBException {
        socket.open();
        String response = socket.request("open topic " + id); //TODO replace with debug log
        System.out.println(response);
    }

    public List<Record> list() throws ListDBException {
        String response = socket.request("list");
        System.out.println(response); //TODO replace with debug log
        return parser.parseData(response);
    }
}
